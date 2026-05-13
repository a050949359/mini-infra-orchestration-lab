from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class JobPayload:
    user_id: int
    action: str
    data: str


@dataclass
class JobRequest:
    type: str
    priority: int
    payload: JobPayload


def parse_job_request(body: Any) -> JobRequest | str:
    if not isinstance(body, dict):
        return "request body must be a JSON object"

    if not isinstance(body.get("type"), str) or not body["type"].strip():
        return "type must be a non-empty string"

    if not isinstance(body.get("priority"), int):
        return "priority must be an integer"

    raw_payload = body.get("payload")
    if not isinstance(raw_payload, dict):
        return "payload must be an object"

    if not isinstance(raw_payload.get("user_id"), int):
        return "payload.user_id must be an integer"

    if not isinstance(raw_payload.get("action"), str) or not raw_payload["action"].strip():
        return "payload.action must be a non-empty string"

    if not isinstance(raw_payload.get("data"), str) or not raw_payload["data"].strip():
        return "payload.data must be a non-empty string"

    return JobRequest(
        type=body["type"],
        priority=body["priority"],
        payload=JobPayload(
            user_id=raw_payload["user_id"],
            action=raw_payload["action"],
            data=raw_payload["data"],
        ),
    )
