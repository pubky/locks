#!/usr/bin/env python3
"""Deterministic fake for the payment-only-v2 provider contract."""

import json
import os
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {
    "create_calls": 0,
    "status_calls": 0,
    "last_create": None,
    "last_status": None,
    "signed_create": False,
    "signed_status": False,
}
LOCK = threading.Lock()
TXID = "11" * 32


def reply(handler, status, payload):
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def parse_body(handler):
    size = int(handler.headers.get("Content-Length", "0"))
    return json.loads(handler.rfile.read(size))


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def canonical_time(value):
    rendered = value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if "." in rendered:
        prefix, fraction = rendered[:-1].split(".", 1)
        fraction = fraction.rstrip("0")
        rendered = f"{prefix}.{fraction}Z" if fraction else f"{prefix}Z"
    return rendered


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        del format, args

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
                    create_calls=0,
                    status_calls=0,
                    last_create=None,
                    last_status=None,
                    signed_create=False,
                    signed_status=False,
                )
            reply(self, 200, {"status": "reset"})
            return

        try:
            body = parse_body(self)
        except (ValueError, json.JSONDecodeError):
            reply(self, 400, {"error": "invalid_json"})
            return

        signed = bool(self.headers.get("X-Paykit-Signature"))
        if self.path == "/v2/invoices":
            deadline = parse_time(body["deadline"])
            created = deadline - timedelta(seconds=59)
            proposal_expires = deadline - timedelta(seconds=1)
            with LOCK:
                STATE["create_calls"] += 1
                STATE["last_create"] = body
                STATE["signed_create"] = signed
            reply(
                self,
                200,
                {
                    "invoice_created_at": canonical_time(created),
                    "proposal_expires_at": canonical_time(proposal_expires),
                    "deadline": body["deadline"],
                },
            )
            return

        if self.path == "/v2/invoices/status":
            with LOCK:
                create = STATE["last_create"]
                STATE["status_calls"] += 1
                STATE["last_status"] = body
                STATE["signed_status"] = signed
            if create is None:
                reply(self, 409, {"error": "invoice_not_created"})
                return
            deadline = create["deadline"]
            created = canonical_time(parse_time(deadline) - timedelta(seconds=59))
            proposal_expires = canonical_time(parse_time(deadline) - timedelta(seconds=1))
            if os.environ.get("PAYMENT_V2_FAKE_MODE") == "terminal-rejected":
                reply(
                    self,
                    200,
                    {
                        "creator": create["payment"]["recipient_pubky"],
                        "bundle_id": body["bundle_id"],
                        "request_state": "rejected",
                        "payment_state": "undetected",
                        "invoice_created_at": created,
                        "proposal_expires_at": proposal_expires,
                        "deadline": deadline,
                        "asset": "BTC",
                        "minimum_confirmations": create["payment"]["minimum_confirmations"],
                        "current_sample": None,
                        "first_threshold_satisfied": None,
                    },
                )
                return
            # Current chain state has reorged to undetected, while immutable first
            # threshold evidence was observed exactly at the Locks deadline.
            reply(
                self,
                200,
                {
                    "creator": create["payment"]["recipient_pubky"],
                    "bundle_id": body["bundle_id"],
                    "request_state": "accepted",
                    "payment_state": "undetected",
                    "invoice_created_at": created,
                    "proposal_expires_at": proposal_expires,
                    "deadline": deadline,
                    "asset": "BTC",
                    "minimum_confirmations": create["payment"]["minimum_confirmations"],
                    "current_sample": {
                        "observed_at": deadline,
                        "status": "undetected",
                        "outpoint": None,
                        "confirmations": 0,
                        "amount_matched": False,
                    },
                    "first_threshold_satisfied": {
                        "observed_at": deadline,
                        "status": "confirmed",
                        "outpoint": {"txid": TXID, "vout": 1},
                        "confirmations": create["payment"]["minimum_confirmations"],
                        "amount_matched": True,
                    },
                },
            )
            return

        reply(self, 404, {"error": "not_found"})


if __name__ == "__main__":
    ThreadingHTTPServer(
        ("0.0.0.0", int(os.environ.get("PAYMENT_V2_FAKE_PORT", "18082"))), Handler
    ).serve_forever()
