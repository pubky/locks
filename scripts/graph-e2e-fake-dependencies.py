#!/usr/bin/env python3
"""Deterministic fake folded-auth and app_spec_v0_7 Nexus APIs."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

READER = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky"
STATE = {
    "auth_mode": "pending",
    "nexus_mode": "true",
    "auth_polls": 0,
    "nexus_calls": 0,
    "paykit_calls": 0,
    "last_nexus_path": None,
    "reader": READER,
}
LOCK = threading.Lock()


def reply(handler, status, payload, content_type="application/json"):
    body = payload if isinstance(payload, bytes) else json.dumps(payload, sort_keys=True).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class ControlHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        del format, args
        return

    def do_GET(self):
        if self.path == "/healthz":
            reply(self, 200, {"status": "ok"})
        elif self.path == "/observations":
            with LOCK:
                reply(self, 200, dict(STATE))
        else:
            reply(self, 404, {"error": "not_found"})

    def do_POST(self):
        if self.path == "/reset":
            with LOCK:
                STATE.update(
                    auth_mode="pending",
                    nexus_mode="true",
                    auth_polls=0,
                    nexus_calls=0,
                    paykit_calls=0,
                    last_nexus_path=None,
                    reader=READER,
                )
            reply(self, 200, {"status": "reset"})
            return
        pieces = self.path.strip("/").split("/")
        if len(pieces) == 3 and pieces[0] == "control" and pieces[1] == "reader":
            reader = pieces[2]
            if reader.startswith("pubky") and len(reader) > 5:
                with LOCK:
                    STATE["reader"] = reader
                reply(self, 200, {"dependency": "reader", "status": "configured"})
                return
        if len(pieces) == 3 and pieces[0] == "control":
            dependency, mode = pieces[1:]
            allowed = {
                "auth": {"pending", "approved", "ambiguous", "unavailable"},
                "nexus": {"true", "false", "malformed", "unavailable", "timeout"},
            }
            if dependency in allowed and mode in allowed[dependency]:
                with LOCK:
                    STATE[f"{dependency}_mode"] = mode
                reply(self, 200, {"dependency": dependency, "mode": mode})
                return
        if self.path == "/poll":
            with LOCK:
                STATE["auth_polls"] += 1
                mode = STATE["auth_mode"]
                reader = STATE["reader"]
            if mode == "approved":
                reply(self, 200, {"status": "approved", "reader": reader})
            elif mode == "pending":
                reply(self, 200, {"status": "pending"})
            elif mode == "unavailable":
                reply(self, 503, {"status": "unavailable"})
            else:
                reply(self, 200, {"status": "ambiguous"})
            return
        if self.path.startswith("/paykit"):
            with LOCK:
                STATE["paykit_calls"] += 1
            reply(self, 500, {"error": "paykit_forbidden"})
            return
        reply(self, 404, {"error": "not_found"})


class NexusHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        del format, args
        return

    def do_GET(self):
        if self.path == "/healthz":
            reply(self, 200, {"status": "ok"})
            return
        with LOCK:
            STATE["nexus_calls"] += 1
            STATE["last_nexus_path"] = self.path
            mode = STATE["nexus_mode"]
        if mode == "timeout":
            time.sleep(5)
            return
        if mode == "unavailable":
            reply(self, 503, {"error": "unavailable"})
            return
        if mode == "malformed":
            reply(self, 200, b"{not-json", "application/json")
            return
        path = urlsplit(self.path).path
        value = mode == "true"
        if "/relationship/" in path:
            reply(self, 200, {"following": value})
        elif "/taggers/" in path:
            reply(self, 200, {"relationship": value, "users": []})
        else:
            reply(self, 404, {"error": "not_found"})


def serve(port, handler):
    ThreadingHTTPServer(("0.0.0.0", port), handler).serve_forever()


if __name__ == "__main__":
    nexus = threading.Thread(target=serve, args=(18081, NexusHandler), daemon=True)
    nexus.start()
    serve(18080, ControlHandler)
