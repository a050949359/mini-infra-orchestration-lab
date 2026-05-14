#!/usr/bin/env python3
import json
import sqlite3
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import redis
from config import load_app_config
from flask import Flask, g, jsonify, request
from models import parse_job_request


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_app() -> Flask:
    app = Flask(__name__)
    cfg = load_app_config()
    app.config["DB_PATH"] = cfg.db_path
    app.config["REDIS_URL"] = cfg.redis_url
    app.config["SNMP_REDIS_URL"] = cfg.snmp_redis_url
    app.config["QUEUE_STREAM_KEY"] = cfg.queue_stream_key

    def get_db() -> sqlite3.Connection:
        if "db" not in g:
            g.db = sqlite3.connect(app.config["DB_PATH"])
            g.db.row_factory = sqlite3.Row
        return g.db

    def get_redis() -> redis.Redis:
        if "redis" not in g:
            g.redis = redis.Redis.from_url(app.config["REDIS_URL"], decode_responses=True)
        return g.redis

    def get_snmp_redis() -> redis.Redis:
        if "snmp_redis" not in g:
            g.snmp_redis = redis.Redis.from_url(app.config["SNMP_REDIS_URL"], decode_responses=True)
        return g.snmp_redis

    @app.teardown_appcontext
    def close_db(_exc: BaseException | None) -> None:
        db = g.pop("db", None)
        if db is not None:
            db.close()

        redis_client = g.pop("redis", None)
        if redis_client is not None:
            redis_client.close()

        snmp_redis = g.pop("snmp_redis", None)
        if snmp_redis is not None:
            snmp_redis.close()

    def init_db() -> None:
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT,
                run_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        db.commit()

    @app.get("/healthz")
    def healthz() -> Any:
        try:
            get_redis().ping()
            redis_status = "ok"
        except redis.RedisError:
            redis_status = "unavailable"
        return jsonify({"status": "ok", "redis": redis_status})

    @app.post("/api/v1/jobs")
    def enqueue_job() -> Any:
        body = request.get_json(silent=True) or {}
        result = parse_job_request(body)
        if isinstance(result, str):
            return jsonify({"error": result}), 400
        req = result

        job_id = str(uuid.uuid4())
        run_id = body.get("run_id") or None
        now = utc_now()

        db = get_db()
        db.execute(
            "INSERT INTO jobs (id, status, payload, run_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps(asdict(req), ensure_ascii=False), run_id, now, now),
        )
        db.commit()

        stream_message = {
            "job_id": job_id,
            "status": "queued",
            "type": req.type,
            "priority": str(req.priority),
            "payload": json.dumps(asdict(req.payload), ensure_ascii=False),
            "created_at": now,
        }

        try:
            stream_entry_id = get_redis().xadd(
                app.config["QUEUE_STREAM_KEY"],
                stream_message,
                maxlen=50000,
                approximate=True,
            )
        except redis.RedisError as exc:
            failed_at = utc_now()
            db.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                ("publish_failed", failed_at, job_id),
            )
            db.commit()
            app.logger.exception("failed to append job to redis stream")
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "publish_failed",
                        "error": "failed to append job to redis stream",
                        "error_detail": str(exc),
                    }
                ),
                503,
            )

        return (
            jsonify(
                {
                    "job_id": job_id,
                    "status": "queued",
                    "created_at": now,
                    "stream": app.config["QUEUE_STREAM_KEY"],
                    "stream_entry_id": stream_entry_id,
                }
            ),
            202,
        )

    @app.get("/api/v1/jobs/<job_id>")
    def get_job_status(job_id: str) -> Any:
        db = get_db()
        row = db.execute(
            "SELECT id, status, payload, run_id, created_at, updated_at FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()

        if row is None:
            return jsonify({"error": "job not found", "job_id": job_id}), 404

        job_body = json.loads(row["payload"]) if row["payload"] else {}

        return jsonify(
            {
                "job_id": row["id"],
                "status": row["status"],
                "run_id": row["run_id"],
                "type": job_body.get("type"),
                "priority": job_body.get("priority"),
                "payload": job_body.get("payload"),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )

    @app.post("/api/v1/jobs/<job_id>/status")
    def update_job_status(job_id: str) -> Any:
        body = request.get_json(silent=True) or {}
        new_status = body.get("status")

        allowed = {"queued", "processing", "done", "failed", "publish_failed"}
        if new_status not in allowed:
            return (
                jsonify({"error": "invalid status", "allowed": sorted(allowed)}),
                400,
            )

        db = get_db()
        cur = db.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, utc_now(), job_id),
        )
        db.commit()

        if cur.rowcount == 0:
            return jsonify({"error": "job not found", "job_id": job_id}), 404

        return jsonify({"job_id": job_id, "status": new_status})

    _SNMP_NODES = ["node1", "node2"]
    _SNMP_METRICS = ["cpu_load1", "mem_total_kb", "mem_avail_kb"]

    @app.get("/api/v1/snmp")
    def get_snmp() -> Any:
        rdb = get_snmp_redis()
        is_latest = "latest" in request.args

        if is_latest:
            data: dict[str, Any] = {}
            for node in _SNMP_NODES:
                snapshot: dict[str, Any] = {}
                latest_ts: float | None = None
                for metric in _SNMP_METRICS:
                    key = f"snmp:{node}:{metric}"
                    entries = rdb.zrevrangebyscore(key, "+inf", "-inf", start=0, num=1, withscores=True)
                    if entries:
                        member, ts = entries[0]
                        snapshot[metric] = float(member.split(":", 1)[1])
                        if latest_ts is None or ts > latest_ts:
                            latest_ts = ts
                if latest_ts is not None:
                    snapshot["ts"] = datetime.fromtimestamp(latest_ts, timezone.utc).isoformat()
                data[node] = snapshot
            return jsonify({"mode": "latest", "data": data})

        now = datetime.now(timezone.utc).timestamp()
        start_ts = now - 7200
        data = {}
        for node in _SNMP_NODES:
            metrics_by_ts: dict[float, dict] = {}
            for metric in _SNMP_METRICS:
                key = f"snmp:{node}:{metric}"
                for member, ts in rdb.zrangebyscore(key, start_ts, now, withscores=True):
                    val = member.split(":", 1)[1]
                    metrics_by_ts.setdefault(ts, {
                        "ts": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                    })[metric] = float(val)
            data[node] = sorted(metrics_by_ts.values(), key=lambda s: s["ts"])
        return jsonify({"mode": "history", "data": data})

    @app.get("/api/v1/jobs/stats")
    def get_jobs_stats() -> Any:
        db = get_db()
        run_id = request.args.get("run_id")

        if run_id:
            rows = db.execute(
                "SELECT status, COUNT(*) AS cnt FROM jobs WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
            by_status = {row["status"]: row["cnt"] for row in rows}
            total = sum(by_status.values())
            in_flight = by_status.get("queued", 0) + by_status.get("processing", 0)
            return jsonify({
                "runs": [{
                    "run_id": run_id,
                    "total": total,
                    "in_flight": in_flight,
                    "is_complete": in_flight == 0 and total > 0,
                    "by_status": by_status,
                }]
            })

        rows = db.execute(
            "SELECT run_id, status, COUNT(*) AS cnt FROM jobs"
            " WHERE run_id IS NOT NULL GROUP BY run_id, status",
        ).fetchall()

        agg: dict[str, dict] = {}
        for row in rows:
            rid = row["run_id"]
            agg.setdefault(rid, {})
            agg[rid][row["status"]] = row["cnt"]

        runs = []
        for rid, by_status in agg.items():
            total = sum(by_status.values())
            in_flight = by_status.get("queued", 0) + by_status.get("processing", 0)
            runs.append({
                "run_id": rid,
                "total": total,
                "in_flight": in_flight,
                "is_complete": in_flight == 0 and total > 0,
                "by_status": by_status,
            })

        return jsonify({"runs": runs})

    @app.get("/api/v1/stats")
    def get_stats() -> Any:
        rdb = get_redis()

        global_raw = rdb.hgetall("worker:stats")
        global_stats = {k: int(v) for k, v in global_raw.items()}

        db = get_db()
        status_rows = db.execute(
            "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
        ).fetchall()
        by_status = {row["status"]: row["cnt"] for row in status_rows}

        result: dict[str, Any] = {
            "global": global_stats,
            "by_status": by_status,
            "queue_pending": by_status.get("queued", 0) + by_status.get("processing", 0),
        }

        run_id = request.args.get("run_id")
        if run_id:
            db = get_db()
            rows = db.execute(
                "SELECT status, COUNT(*) AS cnt FROM jobs WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
            result["run"] = {
                "run_id": run_id,
                "by_status": {row["status"]: row["cnt"] for row in rows},
            }

        return jsonify(result)

    with app.app_context():
        init_db()

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
