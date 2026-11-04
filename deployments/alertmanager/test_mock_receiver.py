import json
import os
import socket
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

SCRIPT = Path(__file__).with_name("mock-alert-receiver.py")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        env = dict(os.environ, ALERT_RECEIVER_HOST="127.0.0.1", ALERT_RECEIVER_PORT=str(self.port))
        self.proc = subprocess.Popen([sys.executable, str(SCRIPT)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.base = f"http://127.0.0.1:{self.port}"
        for _ in range(50):
            try:
                OPENER.open(self.base + "/events", timeout=.1).close()
                break
            except OSError:
                time.sleep(.02)
        else:
            self.fail("receiver did not start")

    def tearDown(self):
        self.proc.terminate()
        self.proc.wait(timeout=2)

    def post(self, status):
        data = json.dumps({"alerts": [{"status": status, "labels": {"alertname": "ForgeQueueTargetDown", "severity": "critical"}, "annotations": {"summary": "target down", "runbook": "deployments/RUNBOOK.md#target-or-dependency-down"}}]}).encode()
        req = urllib.request.Request(self.base + "/alerts", data=data, headers={"Content-Type": "application/json"})
        return OPENER.open(req, timeout=1).status

    def test_firing_and_resolution_are_recorded(self):
        self.assertEqual(self.post("firing"), 200)
        self.assertEqual(self.post("resolved"), 200)
        events = json.load(OPENER.open(self.base + "/events", timeout=1))
        self.assertEqual([event["status"] for event in events], ["firing", "resolved"])
        self.assertEqual(events[0]["name"], "ForgeQueueTargetDown")
        self.assertEqual(events[0]["runbook"], "deployments/RUNBOOK.md#target-or-dependency-down")

    def test_rejects_bad_path_and_unbounded_alert_batch(self):
        with self.assertRaises(urllib.error.HTTPError) as err:
            OPENER.open(self.base + "/wrong", timeout=1)
        self.assertEqual(err.exception.code, 404)
        data = json.dumps({"alerts": [{}] * 101}).encode()
        req = urllib.request.Request(self.base + "/alerts", data=data)
        with self.assertRaises(urllib.error.HTTPError) as err:
            OPENER.open(req, timeout=1)
        self.assertEqual(err.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
