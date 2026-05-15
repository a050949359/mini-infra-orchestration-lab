#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request

_HERE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, instance_path=_HERE)

_runs: dict[str, dict] = {}
_lock = threading.Lock()

SCRIPT_DIR = os.path.realpath(os.getenv("LOADTEST_SCRIPT_DIR", "/opt/mini-orch-loadtest"))


def _execute(run_id: str, script: str, extra_args: list[str], api_url: str) -> None:
    cmd = ["k6", "run", "--no-color"] + extra_args + [script]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, "API_URL": api_url, "RUN_ID": run_id},
        )
        output = proc.stdout + proc.stderr
        exit_code = proc.returncode
    except Exception as exc:
        output = str(exc)
        exit_code = -1

    finished_at = datetime.now(timezone.utc).isoformat()

    with _lock:
        if run_id in _runs:
            _runs[run_id].update({
                "status": "done" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "k6_output": output,
                "finished_at": finished_at,
            })


@app.post("/api/v1/loadtest/runs")
def start_run() -> Any:
    body = request.get_json(silent=True) or {}

    script_name = body.get("script", "api_stress.js")
    script_path = os.path.realpath(os.path.join(SCRIPT_DIR, script_name))
    if not script_path.startswith(SCRIPT_DIR + os.sep):
        return jsonify({"error": "invalid script path"}), 400
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
        }

    threading.Thread(
        target=_execute,
        args=(run_id, script_path, extra_args, api_url),
        daemon=True,
    ).start()

    return jsonify({"run_id": run_id, "status": "running", "started_at": now}), 202


@app.get("/api/v1/loadtest/runs/<run_id>")
def get_run(run_id: str) -> Any:
    with _lock:
        run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "run not found", "run_id": run_id}), 404

    if run["status"] == "running":
        return jsonify({"run_id": run_id, "status": "running", "started_at": run["started_at"]}), 202

    return jsonify(run)


if __name__ == "__main__":
    host = os.getenv("LOADTEST_BIND_HOST") or "127.0.0.1"
    port = int(os.getenv("LOADTEST_PORT", "5001"))
    ssl_cert = os.getenv("SSL_CERT")
    ssl_key = os.getenv("SSL_KEY")
    ssl_context = (ssl_cert, ssl_key) if ssl_cert and ssl_key else None
    app.run(host=host, port=port, ssl_context=ssl_context)
