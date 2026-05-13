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

    @app.teardown_appcontext
    def close_db(_exc: BaseException | None) -> None:
        db = g.pop("db", None)
        if db is not None:
            db.close()

        redis_client = g.pop("redis", None)
        if redis_client is not None:
            redis_client.close()

    def init_db() -> None:
        db = get_db()
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT,
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
        now = utc_now()

        db = get_db()
        db.execute(
            "INSERT INTO jobs (id, status, payload, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps(asdict(req), ensure_ascii=False), now, now),
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
            "SELECT id, status, payload, created_at, updated_at FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()

        if row is None:
            return jsonify({"error": "job not found", "job_id": job_id}), 404

        job_body = json.loads(row["payload"]) if row["payload"] else {}

        return jsonify(
            {
                "job_id": row["id"],
                "status": row["status"],
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

    with app.app_context():
        init_db()

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
