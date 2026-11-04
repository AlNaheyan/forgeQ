#!/usr/bin/env python3
"""Local-only Alertmanager webhook sink; stores bounded lifecycle summaries."""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock

events = []
guard = Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/events":
            self.send_error(404)
            return
        with guard:
            body = json.dumps(events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/alerts":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > 1_000_000:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length))
            alerts = payload.get("alerts")
            if not isinstance(alerts, list) or len(alerts) > 100:
                raise ValueError("invalid alert list")
            summaries = []
            for alert in alerts:
                labels = alert.get("labels", {})
                annotations = alert.get("annotations", {})
                summaries.append({
                    "status": alert.get("status"),
                    "name": labels.get("alertname"),
                    "severity": labels.get("severity"),
                    "summary": annotations.get("summary"),
                    "runbook": annotations.get("runbook"),
                })
            with guard:
                events.extend(summaries)
                del events[:-500]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        except (ValueError, TypeError, json.JSONDecodeError):
            self.send_error(400)

    def log_message(self, *_args):
        return


HTTPServer((os.getenv("ALERT_RECEIVER_HOST", "127.0.0.1"), int(os.getenv("ALERT_RECEIVER_PORT", "8088"))), Handler).serve_forever()
