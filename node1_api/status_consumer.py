#!/usr/bin/env python3
import logging
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone

import redis
from config import load_consumer_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

ALLOWED_STATUSES = {"queued", "processing", "done", "failed", "publish_failed", "dead"}
WORKER_STATS_KEY = "worker:stats"
STAT_MAP = {
    "done":  "processed",
    "failed": "failed",
    "dead":  "dead",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_group(rdb: redis.Redis, stream: str, group: str, start_id: str = "0") -> None:
    try:
        rdb.xgroup_create(stream, group, id=start_id, mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def main() -> None:
    cfg = load_consumer_config()

    rdb = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    rdb.ping()
    log.info("redis connected")

    ensure_group(rdb, cfg.status_stream_key, cfg.group)
    ensure_group(rdb, cfg.queue_stream_key, cfg.group, start_id="$")
    log.info("consumer started: status_stream=%s queue_stream=%s group=%s consumer=%s",
             cfg.status_stream_key, cfg.queue_stream_key, cfg.group, cfg.consumer)

    db = sqlite3.connect(cfg.db_path)

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
                groupname=cfg.group,
                consumername=cfg.consumer,
                streams={cfg.status_stream_key: ">", cfg.queue_stream_key: ">"},
                count=10,
                block=cfg.block_ms,
            )
        except redis.RedisError as e:
            log.error("xreadgroup error: %s", e)
            time.sleep(1)
            continue

        if not results:
            continue

        for stream_name, messages in results:
            for msg_id, fields in messages:
                job_id = fields.get("job_id", "")

                if stream_name == cfg.queue_stream_key:
                    try:
                        cur = db.execute(
                            "UPDATE jobs SET status = 'processing', updated_at = ? WHERE id = ?",
                            (utc_now(), job_id),
                        )
                        db.commit()
                    except sqlite3.Error as e:
                        log.error("db update failed msg_id=%s job_id=%s: %s", msg_id, job_id, e)
                        continue
                    if cur.rowcount == 0:
                        log.warning("job not found msg_id=%s job_id=%s", msg_id, job_id)
                    rdb.xack(cfg.queue_stream_key, cfg.group, msg_id)
                    log.info("updated job_id=%s status=processing", job_id)
                    continue

                status = fields.get("status", "")
                if status not in ALLOWED_STATUSES:
                    log.warning("unknown status, skipping msg_id=%s job_id=%s status=%s", msg_id, job_id, status)
                    rdb.xack(cfg.status_stream_key, cfg.group, msg_id)
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

                rdb.xack(cfg.status_stream_key, cfg.group, msg_id)
                log.info("updated job_id=%s status=%s", job_id, status)

                if stat_field := STAT_MAP.get(status):
                    try:
                        rdb.hincrby(WORKER_STATS_KEY, stat_field, 1)
                    except redis.RedisError as e:
                        log.warning("stats incr failed field=%s: %s", stat_field, e)

    db.close()
    rdb.close()
    log.info("shutdown complete")


if __name__ == "__main__":
    main()
