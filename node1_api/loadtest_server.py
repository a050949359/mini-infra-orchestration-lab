#!/usr/bin/env python3
import os

from flask import Flask
from loadtest import bp

app = Flask(__name__)
app.register_blueprint(bp)

if __name__ == "__main__":
    host = os.getenv("LOADTEST_BIND_HOST") or "127.0.0.1"
    port = int(os.getenv("LOADTEST_PORT", "5001"))
    app.run(host=host, port=port)
