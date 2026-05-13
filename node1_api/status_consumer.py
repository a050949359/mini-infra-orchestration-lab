#!/usr/bin/env python3
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

ALLOWED_STATUSES = {"queued", "processing", "done", "failed", "publish_failed"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_redis_url() -> str:
    url = os.getenv("REDIS_URL")
    if url:
        return url
    password = os.getenv("REDIS_PASSWORD", "IntraNet-Redis-2026!ChangeMe")
    return f"redis://:{password}@127.0.0.1:6379/0"


def ensure_group(rdb: redis.Redis, stream: str, group: str) -> None:
    try:
        rdb.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def main() -> None:
    db_path = os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db")
    stream_key = os.getenv("STATUS_STREAM_KEY", "jobs:status")
    group = os.getenv("STATUS_GROUP", "status-updaters")
    consumer = os.getenv("STATUS_CONSUMER", "node1-status-consumer")
    block_ms = int(os.getenv("STATUS_BLOCK_MS", "5000"))

    rdb = redis.Redis.from_url(build_redis_url(), decode_responses=True)
    rdb.ping()
    log.info("redis connected")

    ensure_group(rdb, stream_key, group)
    log.info("consumer started: stream=%s group=%s consumer=%s", stream_key, group, consumer)

    db = sqlite3.connect(db_path)

    running = True

    def handle_signal(sig, _frame):
        nonlocal running
        log.info("received signal %s, shutting down", sig)
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while running:
        try:
            results = rdb.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream_key: ">"},
                count=10,
                block=block_ms,
            )
        except redis.RedisError as e:
            log.error("xreadgroup error: %s", e)
            time.sleep(1)
            continue

        if not results:
            continue

        for _stream, messages in results:
            for msg_id, fields in messages:
                job_id = fields.get("job_id", "")
                status = fields.get("status", "")

                if status not in ALLOWED_STATUSES:
                    log.warning("unknown status, skipping msg_id=%s job_id=%s status=%s", msg_id, job_id, status)
                    rdb.xack(stream_key, group, msg_id)
                    continue

                try:
                    cur = db.execute(
                        "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                        (status, utc_now(), job_id),
                    )
                    db.commit()
                except sqlite3.Error as e:
                    log.error("db update failed msg_id=%s job_id=%s: %s", msg_id, job_id, e)
                    continue  # 不 XACK，讓 PEL 等 retry

                if cur.rowcount == 0:
                    log.warning("job not found msg_id=%s job_id=%s", msg_id, job_id)

                rdb.xack(stream_key, group, msg_id)
                log.info("updated job_id=%s status=%s", job_id, status)

    db.close()
    rdb.close()
    log.info("shutdown complete")


if __name__ == "__main__":
    main()
