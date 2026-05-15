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
from markupsafe import escape
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
                        "error": "internal server error",
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

    _STATUS_COLOR = {
        "done": "#3fb950",
        "queued": "#e3b341",
        "processing": "#58a6ff",
        "failed": "#f85149",
        "dead": "#bc8cff",
        "publish_failed": "#f85149",
    }
    _PB_COLOR = {
        "done": "#3fb950",
        "processing": "#58a6ff",
        "queued": "#e3b341",
        "failed": "#f85149",
        "dead": "#bc8cff",
    }

    @app.get("/dashboard")
    def dashboard() -> Any:
        from flask import Response

        db = get_db()
        rdb = get_redis()

        # --- job stats ---
        status_rows = db.execute(
            "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
        ).fetchall()
        by_status: dict[str, int] = {r["status"]: r["cnt"] for r in status_rows}
        total_jobs = sum(by_status.values())
        in_flight = by_status.get("queued", 0) + by_status.get("processing", 0)

        global_raw = rdb.hgetall("worker:stats")
        global_stats = {k: int(v) for k, v in global_raw.items()}

        # --- run stats ---
        run_rows = db.execute(
            "SELECT run_id, status, COUNT(*) AS cnt FROM jobs"
            " WHERE run_id IS NOT NULL GROUP BY run_id, status"
        ).fetchall()
        run_agg: dict[str, dict] = {}
        for r in run_rows:
            run_agg.setdefault(r["run_id"], {})[r["status"]] = r["cnt"]
        runs = []
        for rid, bs in run_agg.items():
            t = sum(bs.values())
            inf = bs.get("queued", 0) + bs.get("processing", 0)
            runs.append({"run_id": rid, "total": t, "in_flight": inf,
                         "is_complete": inf == 0 and t > 0, "by_status": bs})
        runs.sort(key=lambda x: x["total"], reverse=True)

        # --- snmp (reuse existing get_snmp() 2h history) ---
        snmp_hist: dict[str, list[dict]] = get_snmp().get_json()["data"]

        # --- render ---
        def sc(s: str) -> str:
            return _STATUS_COLOR.get(s, "#8b949e")

        def stat_rows_html(bs: dict) -> str:
            lines = []
            for s in sorted(bs):
                lines.append(
                    f'<div class="row"><span class="lbl">{escape(s)}</span>'
                    f'<span style="color:{sc(s)}">{bs[s]}</span></div>'
                )
            return "".join(lines)

        def pbar_html(bs: dict, total: int) -> str:
            segs = []
            for s in ("done", "processing", "queued", "failed", "dead"):
                pct = round((bs.get(s, 0) / max(total, 1)) * 100)
                if pct:
                    color = _PB_COLOR.get(s, "#8b949e")
                    segs.append(f'<div style="width:{pct}%;background:{color}"></div>')
            return "".join(segs)

        _NODE_COLORS = {"node1": "#58a6ff", "node2": "#f78166"}

        def sparkline(series: dict[str, list[tuple[float, float]]], unit: str = "") -> str:
            W, H, pl, pr, pt, pb = 280, 120, 30, 6, 6, 18
            cw, ch = W - pl - pr, H - pt - pb
            all_ts: list[float] = []
            all_v: list[float] = []
            for pts in series.values():
                for ts, v in pts:
                    all_ts.append(ts); all_v.append(v)
            if not all_v:
                return (f'<svg viewBox="0 0 {W} {H}" width="100%" height="{H}">'
                        f'<text x="10" y="20" fill="#484f58" font-size="9">no data</text></svg>')
            ts0, ts1 = min(all_ts), max(all_ts)
            v0, v1 = min(all_v), max(all_v)
            vpad = (v1 - v0) * 0.12 or 0.5
            v0, v1 = v0 - vpad, v1 + vpad
            tspan, vspan = (ts1 - ts0) or 1, (v1 - v0)

            def px(ts: float) -> float: return pl + (ts - ts0) / tspan * cw
            def py(v: float) -> float: return pt + (1 - (v - v0) / vspan) * ch

            out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="100%" height="{H}">']
            for v in (v0, (v0 + v1) / 2, v1):
                y = py(v)
                lbl = f"{v:.1f}{unit}"
                out.append(f'<line x1="{pl}" y1="{y:.1f}" x2="{W-pr}" y2="{y:.1f}" stroke="#21262d" stroke-width="1"/>')
                out.append(f'<text x="{pl-2}" y="{y+3:.1f}" text-anchor="end" font-size="7.5" fill="#484f58">{lbl}</text>')
            for node, pts in sorted(series.items()):
                if len(pts) < 2:
                    continue
                color = _NODE_COLORS.get(node, "#8b949e")
                coords = " ".join(f"{px(ts):.1f},{py(v):.1f}" for ts, v in pts)
                out.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>')
            lx = pl
            for node in sorted(series):
                color = _NODE_COLORS.get(node, "#8b949e")
                out.append(f'<rect x="{lx}" y="{H-7}" width="12" height="3" rx="1" fill="{color}"/>')
                out.append(f'<text x="{lx+14}" y="{H-4}" font-size="7.5" fill="#8b949e">{escape(node)}</text>')
                lx += 65
            out.append('</svg>')
            return "".join(out)

        stats_html = (
            f'<div class="row"><span class="lbl">total</span><span>{total_jobs}</span></div>'
            + stat_rows_html(by_status)
            + f'<div class="row" style="margin-top:6px"><span class="lbl">in_flight</span>'
              f'<span style="color:#e3b341">{in_flight}</span></div>'
        )
        if global_stats:
            for k, v in sorted(global_stats.items()):
                stats_html += (
                    f'<div class="row"><span class="lbl">worker:{escape(k)}</span>'
                    f'<span style="color:#8b949e">{v}</span></div>'
                )

        cpu_series: dict[str, list[tuple[float, float]]] = {}
        mem_series: dict[str, list[tuple[float, float]]] = {}
        for node, rows in snmp_hist.items():
            cpu_pts, mem_pts = [], []
            for row in rows:
                try:
                    ts = datetime.fromisoformat(row["ts"]).timestamp()
                except (KeyError, ValueError):
                    continue
                if "cpu_load1" in row:
                    cpu_pts.append((ts, row["cpu_load1"]))
                total = row.get("mem_total_kb", 0)
                if total > 0 and "mem_avail_kb" in row:
                    free = row["mem_avail_kb"]
                    buf = row.get("mem_buffer_kb", 0)
                    cached = row.get("mem_cached_kb", 0)
                    mem_pts.append((ts, (total - free - buf - cached) / total * 100))
            cpu_series[node] = cpu_pts
            mem_series[node] = mem_pts

        snmp_html = (
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">'
            f'<div><div class="snmp-cl">cpu_load1</div>{sparkline(cpu_series)}</div>'
            f'<div><div class="snmp-cl">mem_used %</div>{sparkline(mem_series, unit="%")}</div>'
            f'</div>'
        )

        runs_html_parts = []
        for r in runs:
            bs = r["by_status"]
            badge = '<span class="badge-ok">done</span>' if r["is_complete"] else ""
            detail = stat_rows_html(bs) + (
                f'<div class="row"><span class="lbl">in_flight</span>'
                f'<span style="color:#e3b341">{r["in_flight"]}</span></div>'
            )
            pb = pbar_html(bs, r["total"])
            runs_html_parts.append(
                f'<details><summary style="cursor:pointer;list-style:none">'
                f'<div class="run-sum">'
                f'<span class="rid">{escape(r["run_id"])}</span>'
                f'<span class="lbl">{r["total"]} jobs</span>{badge}'
                f'</div>'
                f'<div class="pbar">{pb}</div>'
                f'</summary>'
                f'<div style="padding:4px 0 4px 12px;font-size:.85em">{detail}</div>'
                f'</details>'
            )
        runs_html = "".join(runs_html_parts) or '<span class="ts">no runs</span>'

        now_str = utc_now()
        html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<title>Mini Orch Dashboard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:monospace;background:#0f1117;color:#c9d1d9;padding:14px;font-size:14px}}
h1{{color:#58a6ff;font-size:1.1em;margin-bottom:12px}}
h3{{color:#79c0ff;font-size:.9em;margin-bottom:8px;text-transform:uppercase;letter-spacing:.05em}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px}}
.row{{display:flex;justify-content:space-between;padding:2px 0}}
.lbl{{color:#8b949e}}
.badge-ok{{background:#1a3a1a;color:#3fb950;font-size:.75em;padding:1px 7px;border-radius:10px;border:1px solid #3fb950}}
.run-sum{{display:flex;justify-content:space-between;align-items:center;gap:8px}}
.rid{{color:#58a6ff;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.pbar{{height:6px;background:#21262d;border-radius:3px;display:flex;overflow:hidden;margin:5px 0}}
.snmp-cl{{color:#79c0ff;font-size:.8em;margin-bottom:3px}}
.ts{{color:#484f58;font-size:.8em}}
details{{padding:4px 0;border-bottom:1px solid #21262d}}
details:last-child{{border-bottom:none}}
details summary{{outline:none}}
details summary::-webkit-details-marker{{display:none}}
.generated{{color:#484f58;font-size:.75em;text-align:right;margin-top:8px}}
</style>
</head>
<body>
<h1>Mini Orch Dashboard</h1>
<div class="card" style="margin-bottom:10px"><h3>SNMP</h3>{snmp_html}</div>
<div class="card" style="margin-bottom:10px"><h3>Job Status</h3>{stats_html}</div>
<div class="card"><h3>Runs</h3>{runs_html}</div>
<div class="generated">generated {now_str}</div>
</body>
</html>"""

        return Response(html, mimetype="text/html", headers={"X-Frame-Options": "ALLOWALL"})

    with app.app_context():
        init_db()

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
