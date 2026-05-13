from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    db_path: str
    redis_url: str
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


def load_app_config() -> AppConfig:
    return AppConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=os.getenv("REDIS_URL", ""),
        queue_stream_key=os.getenv("QUEUE_STREAM_KEY", "jobs:stream"),
    )


def load_consumer_config() -> ConsumerConfig:
    return ConsumerConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=os.getenv("REDIS_URL", ""),
        status_stream_key=os.getenv("STATUS_STREAM_KEY", "jobs:status"),
        group=os.getenv("STATUS_GROUP", "status-updaters"),
        consumer=os.getenv("STATUS_CONSUMER", "node1-status-consumer"),
        block_ms=int(os.getenv("STATUS_BLOCK_MS", "5000")),
    )


def load_checker_config() -> CheckerConfig:
    return CheckerConfig(
        db_path=os.getenv("JOB_DB_PATH", "/tmp/node1_jobs.db"),
        redis_url=os.getenv("REDIS_URL", ""),
        queue_stream_key=os.getenv("QUEUE_STREAM_KEY", "jobs:stream"),
        stuck_threshold_sec=int(os.getenv("STUCK_JOB_THRESHOLD_SEC", "300")),
    )
