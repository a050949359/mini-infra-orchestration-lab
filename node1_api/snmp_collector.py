#!/usr/bin/env python3
from __future__ import annotations

import logging
import subprocess
import time

import redis

from config import SnmpConfig, load_snmp_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

def snmp_get(host: str, oid: str, community: str) -> float | None:
    try:
        r = subprocess.run(
            ["snmpget", "-v2c", "-c", community, "-Oqv", host, oid],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            return float(r.stdout.strip().strip('"'))
    except Exception as exc:
        log.debug("snmpget failed host=%s oid=%s: %s", host, oid, exc)
    return None


def collect(rdb: redis.Redis, cfg: SnmpConfig) -> None:
    ts = time.time()
    pipe = rdb.pipeline()

    for label, host in cfg.nodes:
        for metric, oid in cfg.oids.items():
            val = snmp_get(host, oid, cfg.community)
            if val is None:
                continue
            key = f"snmp:{label}:{metric}"
            pipe.zadd(key, {f"{ts:.3f}:{val}": ts})
            pipe.expire(key, cfg.retention_sec)

    pipe.execute()


def main() -> None:
    cfg = load_snmp_config()
    rdb = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    log.info(
        "snmp_collector started nodes=%s interval=%ds retention=%dh",
        [n for n, _ in cfg.nodes], cfg.poll_interval, cfg.retention_sec // 3600,
    )

    while True:
        try:
            collect(rdb, cfg)
        except Exception as exc:
            log.error("collect error: %s", exc)
        time.sleep(cfg.poll_interval)


if __name__ == "__main__":
    main()
