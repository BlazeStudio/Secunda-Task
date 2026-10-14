"""Smoke test against a running docker compose stack. Standard library only.

docker compose up --build -d
python scripts/smoke_test.py
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

API_URL = os.environ.get("API_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "dev-api-key-change-me")
RABBITMQ_API = os.environ.get("RABBITMQ_API", "http://localhost:15672/api")

# addresses as seen from inside the compose network
GOOD_WEBHOOK = "http://webhook-receiver:8080/payments"
DEAD_WEBHOOK = "http://webhook-receiver:9/payments"  # nothing listens there


def request(
    method: str, url: str, body: Any = None, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, str], Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status, raw_headers, payload = resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as exc:
        status, raw_headers, payload = exc.code, exc.headers, exc.read()
    headers = {name.lower(): value for name, value in raw_headers.items()}
    return status, headers, json.loads(payload or b"null")


def api(
    method: str, path: str, body: Any = None, **headers: str
) -> tuple[int, dict[str, str], Any]:
    return request(method, API_URL + path, body, {"X-API-Key": API_KEY, **headers})


def create_payment(webhook_url: str, key: str) -> tuple[int, dict[str, str], Any]:
    body = {
        "amount": "250.00",
        "currency": "EUR",
        "description": "smoke test",
        "metadata": {"source": "smoke_test"},
        "webhook_url": webhook_url,
    }
    return api("POST", "/api/v1/payments", body, **{"Idempotency-Key": key})


def wait_for(what: str, check: Any, timeout: float = 60) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(1)
    sys.exit(f"FAIL: timed out waiting for {what}")


def expect(condition: bool, message: str) -> None:
    if not condition:
        sys.exit(f"FAIL: {message}")
    print(f"ok   {message}")


def dlq_depth() -> int:
    token = base64.b64encode(b"guest:guest").decode()
    _, _, queue = request(
        "GET",
        f"{RABBITMQ_API}/queues/%2F/payments.new.dlq",
        headers={"Authorization": f"Basic {token}"},
    )
    return int(queue.get("messages", 0))


def main() -> None:
    status, _, _ = request("POST", API_URL + "/api/v1/payments", {})
    expect(status == 401, "request without API key is rejected")

    key = str(uuid.uuid4())
    status, headers, created = create_payment(GOOD_WEBHOOK, key)
    expect(status == 202 and created["status"] == "pending", "payment accepted")

    status, headers, replay = create_payment(GOOD_WEBHOOK, key)
    expect(
        status == 202
        and replay["payment_id"] == created["payment_id"]
        and headers.get("idempotent-replayed") == "true",
        "retry with the same Idempotency-Key returns the same payment",
    )

    status, _, _ = create_payment(DEAD_WEBHOOK, key)
    expect(status == 409, "same key with a different body is rejected")

    def processed() -> Any:
        _, _, payment = api("GET", f"/api/v1/payments/{created['payment_id']}")
        return payment if payment["status"] != "pending" else None

    payment = wait_for("payment to be processed", processed)
    expect(payment["processed_at"] is not None, f"payment processed: {payment['status']}")

    dlq_before = dlq_depth()
    create_payment(DEAD_WEBHOOK, str(uuid.uuid4()))
    wait_for("message to land in the DLQ", lambda: dlq_depth() > dlq_before, timeout=90)
    print("ok   message with an undeliverable webhook ended up in payments.new.dlq")

    print("smoke test passed")


if __name__ == "__main__":
    main()
