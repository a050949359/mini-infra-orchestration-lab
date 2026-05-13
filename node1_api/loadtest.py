from __future__ import annotations

import os
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, jsonify, request

bp = Blueprint("loadtest", __name__)

_runs: dict[str, dict] = {}
_lock = threading.Lock()

SCRIPT_DIR = os.getenv("LOADTEST_SCRIPT_DIR", "/opt/mini-orch-loadtest")
SNMP_COMMUNITY = os.getenv("SNMP_COMMUNITY", "public")
SNMP_POLL_INTERVAL = int(os.getenv("SNMP_POLL_INTERVAL", "5"))

SNMP_NODES: list[tuple[str, str]] = [("node1", os.getenv("NODE1_HOST", "127.0.0.1"))]
if os.getenv("NODE2_HOST"):
    SNMP_NODES.append(("node2", os.getenv("NODE2_HOST", "")))

SNMP_OIDS = {
    "cpu_load1":   "1.3.6.1.4.1.2021.10.1.3.1",
    "mem_total_kb": "1.3.6.1.4.1.2021.4.5.0",
    "mem_avail_kb": "1.3.6.1.4.1.2021.4.6.0",
}


def _snmp_get(host: str, oid: str) -> float | None:
    try:
        r = subprocess.run(
            ["snmpget", "-v2c", "-c", SNMP_COMMUNITY, "-Oqv", host, oid],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            return float(r.stdout.strip().strip('"'))
    except Exception:
        pass
    return None


def _poll_once() -> dict[str, dict]:
    ts = datetime.now(timezone.utc).isoformat()
    result: dict[str, dict] = {}
    for label, host in SNMP_NODES:
        sample: dict[str, Any] = {"ts": ts}
        for name, oid in SNMP_OIDS.items():
            sample[name] = _snmp_get(host, oid)
        result[label] = sample
    return result


def _snmp_poller(run_id: str, stop: threading.Event) -> None:
    while not stop.wait(SNMP_POLL_INTERVAL):
        samples = _poll_once()
        with _lock:
            if run_id not in _runs:
                break
            for label, sample in samples.items():
                _runs[run_id]["snmp"].setdefault(label, []).append(sample)


def _execute(run_id: str, script: str, extra_args: list[str], api_url: str) -> None:
    stop = threading.Event()
    poller = threading.Thread(target=_snmp_poller, args=(run_id, stop), daemon=True)
    poller.start()

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
    finally:
        stop.set()
        poller.join(timeout=SNMP_POLL_INTERVAL + 2)

    with _lock:
        if run_id in _runs:
            _runs[run_id].update({
                "status": "done" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "k6_output": output,
                "finished_at": datetime.now(timezone.utc).isoformat(),
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
