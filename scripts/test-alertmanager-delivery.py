#!/usr/bin/env python3
"""Run Alertmanager against the local mock and verify firing plus resolution."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "prom/alertmanager:v0.28.1"
MOCK = ROOT / "deployments/alertmanager/mock-alert-receiver.py"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, payload=None):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    return opener.open(req, timeout=3)


def main():
    receiver_port, alertmanager_port = free_port(), free_port()
    name = "forgeq-am-test-" + uuid.uuid4().hex[:8]
    env = dict(os.environ, ALERT_RECEIVER_HOST="0.0.0.0", ALERT_RECEIVER_PORT=str(receiver_port))
    receiver = subprocess.Popen([sys.executable, str(MOCK)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    container = False
    try:
        with tempfile.TemporaryDirectory(prefix="forgeq-am-test-") as temp:
            config = (ROOT / "deployments/alertmanager/alertmanager.yml").read_text()
            config = config.replace("group_wait: 10s", "group_wait: 1s").replace("group_interval: 5m", "group_interval: 1s")
            config = config.replace("host.docker.internal:8088", f"host.docker.internal:{receiver_port}")
            path = Path(temp) / "alertmanager.yml"
            path.write_text(config)
            subprocess.run(["docker", "run", "-d", "--rm", "--name", name,
                            "--add-host=host.docker.internal:host-gateway",
                            "-p", f"127.0.0.1:{alertmanager_port}:9093",
                            "-v", f"{path}:/etc/alertmanager/alertmanager.yml:ro",
                            IMAGE, "--config.file=/etc/alertmanager/alertmanager.yml",
                            "--web.listen-address=:9093"], check=True, stdout=subprocess.DEVNULL)
            container = True
            api = f"http://127.0.0.1:{alertmanager_port}/api/v2/alerts"
            for _ in range(50):
                try:
                    request(f"http://127.0.0.1:{receiver_port}/events")
                    request(f"http://127.0.0.1:{alertmanager_port}/-/healthy")
                    break
                except OSError:
                    time.sleep(.2)
            now = time.time()
            stamp = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
            alert = {"labels": {"alertname": "ForgeQueueDeliveryTest", "severity": "critical", "job": "test"},
                     "annotations": {"summary": "local delivery test", "runbook": "deployments/RUNBOOK.md"},
                     "startsAt": stamp(now - 1), "endsAt": stamp(now + 3), "generatorURL": "http://localhost/test"}
            request(api, [alert]).close()
            deadline = time.time() + 20
            observed = set()
            while time.time() < deadline and observed != {"firing", "resolved"}:
                events = json.load(request(f"http://127.0.0.1:{receiver_port}/events"))
                observed = {event["status"] for event in events if event.get("name") == "ForgeQueueDeliveryTest"}
                time.sleep(.25)
            if observed != {"firing", "resolved"}:
                raise RuntimeError(f"expected firing and resolved delivery; got {sorted(observed)}")
            print("Alertmanager mock delivery passed: firing and resolved notifications received")
    finally:
        if container:
            subprocess.run(["docker", "stop", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        receiver.terminate()
        receiver.wait(timeout=3)


if __name__ == "__main__":
    main()
