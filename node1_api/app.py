#!/usr/bin/env python3
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

import redis
from flask import Flask, g, jsonify, request


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_enqueue_body(body: Any) -> tuple[bool, str | None]:
    if not isinstance(body, dict):
        return False, "request body must be a JSON object"

    required_top = {"type", "payload", "priority"}
    if not required_top.issubset(body.keys()):
        return False, "required fields: type, payload, priority"

    if not isinstance(body.get("type"), str) or not body["type"].strip():
        return False, "type must be a non-empty string"

    if not isinstance(body.get("priority"), int):
        return False, "priority must be an integer"

    payload = body.get("payload")
    if not isinstance(payload, dict):
        return False, "payload must be an object"

    required_payload = {"user_id", "action", "data"}
    if not required_payload.issubset(payload.keys()):
        return False, "payload required fields: user_id, action, data"

    if not isinstance(payload.get("user_id"), int):
        return False, "payload.user_id must be an integer"

    if not isinstance(payload.get("action"), str) or not payload["action"].strip():
        return False, "payload.action must be a non-empty string"

    if not isinstance(payload.get("data"), str) or not payload["data"].strip():
        return False, "payload.data must be a non-empty string"

    return True, None


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db")
    redis_password = os.getenv("REDIS_PASSWORD", "IntraNet-Redis-2026!ChangeMe")
    default_redis_url = f"redis://:{redis_password}@127.0.0.1:6379/0"
    app.config["REDIS_URL"] = os.getenv("REDIS_URL", default_redis_url)
    app.config["QUEUE_STREAM_KEY"] = os.getenv("QUEUE_STREAM_KEY", "jobs:stream")

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

    @app.before_request
    def ensure_table() -> None:
        init_db()

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
        ok, error = validate_enqueue_body(body)
        if not ok:
            return jsonify({"error": error}), 400

        job_id = str(uuid.uuid4())
        now = utc_now()

        db = get_db()
        db.execute(
            "INSERT INTO jobs (id, status, payload, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, "queued", json.dumps(body, ensure_ascii=False), now, now),
        )
        db.commit()

        stream_message = {
            "job_id": job_id,
            "status": "queued",
            "type": body["type"],
            "priority": str(body["priority"]),
            "payload": json.dumps(body["payload"], ensure_ascii=False),
            "created_at": now,
        }

        try:
            stream_entry_id = get_redis().xadd(
                app.config["QUEUE_STREAM_KEY"],
                stream_message,
            )
        except redis.RedisError:
            failed_at = utc_now()
            db.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                ("publish_failed", failed_at, job_id),
            )
            db.commit()
            return (
                jsonify(
                    {
                        "job_id": job_id,
                        "status": "publish_failed",
                        "error": "failed to append job to redis stream",
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

        return jsonify(
            {
                "job_id": row["id"],
                "status": row["status"],
                "payload": json.loads(row["payload"]) if row["payload"] else None,
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
        exists = db.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not exists:
            return jsonify({"error": "job not found", "job_id": job_id}), 404

        db.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, utc_now(), job_id),
        )
        db.commit()

        return jsonify({"job_id": job_id, "status": new_status})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
