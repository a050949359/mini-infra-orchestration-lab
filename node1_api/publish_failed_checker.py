#!/usr/bin/env python3
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import redis
from config import load_checker_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    cfg = load_checker_config()

    rdb = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    rdb.ping()

    db = sqlite3.connect(cfg.db_path)
    db.row_factory = sqlite3.Row

    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=cfg.stuck_threshold_sec)).isoformat()

    rows = db.execute(
        """
        SELECT id, payload, created_at FROM jobs
        WHERE status = 'publish_failed'
           OR (status = 'queued' AND updated_at < ?)
        """,
        (cutoff,),
    ).fetchall()

    if not rows:
        log.info("no stuck jobs found")
        db.close()
        rdb.close()
        return

    log.info("found %d stuck job(s)", len(rows))
    recovered = 0

    for row in rows:
        job_id = row["id"]
        try:
            body = json.loads(row["payload"]) if row["payload"] else {}
        except json.JSONDecodeError:
            log.error("invalid payload job_id=%s, skipping", job_id)
            continue

        stream_message = {
            "job_id": job_id,
            "status": "queued",
            "type": body.get("type", ""),
            "priority": str(body.get("priority", 0)),
            "payload": json.dumps(body.get("payload", {}), ensure_ascii=False),
            "created_at": row["created_at"],
        }

        try:
            rdb.xadd(cfg.queue_stream_key, stream_message)
        except redis.RedisError as e:
            log.error("xadd failed job_id=%s: %s", job_id, e)
            continue

        now = utc_now()
        db.execute(
            "UPDATE jobs SET status = 'queued', updated_at = ? WHERE id = ?",
            (now, job_id),
        )
        db.commit()
        log.info("re-enqueued job_id=%s", job_id)
        recovered += 1

    log.info("done: recovered=%d/%d", recovered, len(rows))
    db.close()
    rdb.close()


if __name__ == "__main__":
    main()
