#!/usr/bin/env python3
"""Transparent Paykit relay that drops first cleanup response."""

import http.client
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM_HOST = os.environ.get("PAYKIT_UPSTREAM_HOST", "paykit")
UPSTREAM_PORT = int(os.environ.get("PAYKIT_UPSTREAM_PORT", "3001"))
STATE = {"cleanup_calls": 0, "cleanup_response_dropped": False}
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        del format, args

    def local_json(self, payload):
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self.local_json({"status": "ok"})
            return
        if self.path == "/observations":
            with LOCK:
                self.local_json(dict(STATE))
            return
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size)
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"host", "content-length", "connection"}
        }
        connection = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=30)
        connection.request(self.command, self.path, body=body, headers=headers)
        response = connection.getresponse()
        response_body = response.read()
        drop = False
        if self.path == "/payment-request-drain-cleanups":
            with LOCK:
                STATE["cleanup_calls"] += 1
                if not STATE["cleanup_response_dropped"]:
                    STATE["cleanup_response_dropped"] = True
                    drop = True
        if drop:
            connection.close()
            self.close_connection = True
            self.connection.shutdown(2)
            self.connection.close()
            return
        self.send_response(response.status)
        for key, value in response.getheaders():
            if key.lower() not in {"connection", "transfer-encoding", "content-length"}:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)
        connection.close()


ThreadingHTTPServer(("0.0.0.0", 3002), Handler).serve_forever()
