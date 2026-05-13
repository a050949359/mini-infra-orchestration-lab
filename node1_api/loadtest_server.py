#!/usr/bin/env python3
import os

from flask import Flask
from loadtest import bp

_HERE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, instance_path=_HERE)
app.register_blueprint(bp)

if __name__ == "__main__":
    host = os.getenv("LOADTEST_BIND_HOST") or "127.0.0.1"
    port = int(os.getenv("LOADTEST_PORT", "5001"))
    app.run(host=host, port=port)
