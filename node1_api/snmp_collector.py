#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import subprocess
import time

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("SNMP_REDIS_URL", "redis://localhost:6379/1")
SNMP_COMMUNITY = os.getenv("SNMP_COMMUNITY", "public")
POLL_INTERVAL = int(os.getenv("SNMP_POLL_INTERVAL", "5"))
RETENTION_SEC = int(os.getenv("SNMP_RETENTION_HOURS", "3")) * 3600

NODES: list[tuple[str, str]] = [("node1", os.getenv("NODE1_HOST", "127.0.0.1"))]
if os.getenv("NODE2_HOST"):
    NODES.append(("node2", os.getenv("NODE2_HOST", "")))

OIDS: dict[str, str] = {
    "cpu_load1":    "1.3.6.1.4.1.2021.10.1.3.1",
    "mem_total_kb": "1.3.6.1.4.1.2021.4.5.0",
    "mem_avail_kb": "1.3.6.1.4.1.2021.4.6.0",
}


def snmp_get(host: str, oid: str) -> float | None:
    try:
        r = subprocess.run(
            ["snmpget", "-v2c", "-c", SNMP_COMMUNITY, "-Oqv", host, oid],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            return float(r.stdout.strip().strip('"'))
    except Exception as exc:
        log.debug("snmpget failed host=%s oid=%s: %s", host, oid, exc)
    return None


def collect(rdb: redis.Redis) -> None:
    ts = time.time()
    pipe = rdb.pipeline()

    for label, host in NODES:
        for metric, oid in OIDS.items():
            val = snmp_get(host, oid)
            if val is None:
                continue
            key = f"snmp:{label}:{metric}"
            pipe.zadd(key, {f"{ts:.3f}:{val}": ts})
            pipe.expire(key, RETENTION_SEC)

    pipe.execute()


def main() -> None:
    rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    log.info(
        "snmp_collector started nodes=%s interval=%ds retention=%dh",
        [n for n, _ in NODES], POLL_INTERVAL, RETENTION_SEC // 3600,
    )

    while True:
        try:
            collect(rdb)
        except Exception as exc:
            log.error("collect error: %s", exc)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
