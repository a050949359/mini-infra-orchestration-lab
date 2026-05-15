from __future__ import annotations

import os
from dataclasses import dataclass


def _build_redis_url(host: str, port: str, db: str, passwd: str) -> str:
    return f"redis://:{passwd}@{host}:{port}/{db}"


def _redis_components() -> tuple[str, str, str, str]:
    return (
        os.getenv("REDIS_HOST", "127.0.0.1"),
        os.getenv("REDIS_PORT", "6379"),
        os.getenv("REDIS_DB", "0"),
        os.getenv("REDIS_PASSWD", ""),
    )


@dataclass
class SnmpConfig:
    redis_url: str
    community: str
    poll_interval: int
    retention_sec: int
    nodes: list[tuple[str, str]]
    oids: dict[str, str]


@dataclass
class AppConfig:
    db_path: str
    redis_url: str
    snmp_redis_url: str
    queue_stream_key: str


@dataclass
class ConsumerConfig:
    db_path: str
    redis_url: str
    status_stream_key: str
    group: str
    consumer: str
    block_ms: int


@dataclass
class CheckerConfig:
    db_path: str
    redis_url: str
    queue_stream_key: str
    stuck_threshold_sec: int


def load_snmp_config() -> SnmpConfig:
    host, port, db, passwd = _redis_components()
    raw_nodes = os.getenv("SNMP_NODES", "node1:127.0.0.1")
    nodes = []
    for _e in raw_nodes.split(","):
        _e = _e.strip()
        if ":" in _e:
            _label, _ip = _e.split(":", 1)
            nodes.append((_label, _ip))
    oids = {}
    for _e in os.getenv("SNMP_OIDS", "").split(","):
        _e = _e.strip()
        if ":" in _e:
            _label, _oid = _e.split(":", 1)
            oids[_label] = _oid
    return SnmpConfig(
        redis_url=_build_redis_url(host, port, db, passwd),
        community=os.getenv("SNMP_COMMUNITY", "public"),
        poll_interval=int(os.getenv("SNMP_POLL_INTERVAL", "5")),
        retention_sec=int(os.getenv("SNMP_RETENTION_HOURS", "3")) * 3600,
        nodes=nodes,
        oids=oids,
    )


def load_app_config() -> AppConfig:
    host, port, db, passwd = _redis_components()
    snmp_db = os.getenv("SNMP_REDIS_DB", "1")
    return AppConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=_build_redis_url(host, port, db, passwd),
        snmp_redis_url=_build_redis_url(host, port, snmp_db, passwd),
        queue_stream_key=os.getenv("QUEUE_STREAM_KEY", "jobs:stream"),
    )


def load_consumer_config() -> ConsumerConfig:
    host, port, db, passwd = _redis_components()
    return ConsumerConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=_build_redis_url(host, port, db, passwd),
        status_stream_key=os.getenv("STATUS_STREAM_KEY", "jobs:status"),
        group=os.getenv("STATUS_GROUP", "status-updaters"),
        consumer=os.getenv("STATUS_CONSUMER", "node1-status-consumer"),
        block_ms=int(os.getenv("STATUS_BLOCK_MS", "5000")),
    )


def load_checker_config() -> CheckerConfig:
    host, port, db, passwd = _redis_components()
    return CheckerConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=_build_redis_url(host, port, db, passwd),
        queue_stream_key=os.getenv("QUEUE_STREAM_KEY", "jobs:stream"),
        stuck_threshold_sec=int(os.getenv("STUCK_JOB_THRESHOLD_SEC", "300")),
    )
