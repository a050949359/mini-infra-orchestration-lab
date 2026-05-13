from __future__ import annotations

import os
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

import redis
from flask import Blueprint, jsonify, request

bp = Blueprint("loadtest", __name__)

_runs: dict[str, dict] = {}
_lock = threading.Lock()

SCRIPT_DIR = os.getenv("LOADTEST_SCRIPT_DIR", "/opt/mini-orch-loadtest")
SNMP_REDIS_URL = os.getenv("SNMP_REDIS_URL", "redis://localhost:6379/1")
SNMP_NODES = ["node1", "node2"]
SNMP_METRICS = ["cpu_load1", "mem_total_kb", "mem_avail_kb"]


def _fetch_snmp(started_at: str, finished_at: str) -> dict[str, Any]:
    start_ts = datetime.fromisoformat(started_at).timestamp()
    end_ts = datetime.fromisoformat(finished_at).timestamp()

    try:
        rdb = redis.Redis.from_url(SNMP_REDIS_URL, decode_responses=True)
        result: dict[str, Any] = {}
        for node in SNMP_NODES:
            metrics_by_ts: dict[float, dict] = {}
            for metric in SNMP_METRICS:
                key = f"snmp:{node}:{metric}"
                for val, ts in rdb.zrangebyscore(key, start_ts, end_ts, withscores=True):
                    metrics_by_ts.setdefault(ts, {
                        "ts": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                    })[metric] = float(val)
            result[node] = sorted(metrics_by_ts.values(), key=lambda s: s["ts"])
        rdb.close()
        return result
    except Exception as exc:
        return {"error": str(exc)}


def _execute(run_id: str, script: str, extra_args: list[str], api_url: str) -> None:
    cmd = ["k6", "run", "--no-color"] + extra_args + [script]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, "API_URL": api_url},
        )
        output = proc.stdout + proc.stderr
        exit_code = proc.returncode
    except Exception as exc:
        output = str(exc)
        exit_code = -1

    finished_at = datetime.now(timezone.utc).isoformat()

    with _lock:
        started_at = _runs[run_id]["started_at"]

    snmp = _fetch_snmp(started_at, finished_at)

    with _lock:
        if run_id in _runs:
            _runs[run_id].update({
                "status": "done" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "k6_output": output,
                "finished_at": finished_at,
                "snmp": snmp,
            })


@bp.post("/api/v1/loadtest/runs")
def start_run() -> Any:
    body = request.get_json(silent=True) or {}

    script_name = body.get("script", "api_stress.js")
    script_path = os.path.join(SCRIPT_DIR, script_name)
    if not os.path.isfile(script_path):
        return jsonify({"error": f"script not found: {script_name}"}), 400

    api_url = body.get("api_url", "http://localhost:5000")

    extra_args: list[str] = []
    if "vus" in body:
        extra_args += ["--vus", str(int(body["vus"]))]
    if "duration" in body:
        extra_args += ["--duration", str(body["duration"])]

    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    with _lock:
        _runs[run_id] = {
            "run_id": run_id,
            "status": "running",
            "script": script_name,
            "api_url": api_url,
            "vus": body.get("vus"),
            "duration": body.get("duration"),
            "started_at": now,
            "finished_at": None,
            "exit_code": None,
            "k6_output": None,
            "snmp": {},
        }

    threading.Thread(
        target=_execute,
        args=(run_id, script_path, extra_args, api_url),
        daemon=True,
    ).start()

    return jsonify({"run_id": run_id, "status": "running", "started_at": now}), 202


@bp.get("/api/v1/loadtest/runs/<run_id>")
def get_run(run_id: str) -> Any:
    with _lock:
        run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "run not found", "run_id": run_id}), 404

    if run["status"] == "running":
        return jsonify({"run_id": run_id, "status": "running", "started_at": run["started_at"]}), 202

    return jsonify(run)
