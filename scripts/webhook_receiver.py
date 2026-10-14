"""Webhook receiver for local runs: plays the role of the merchant's server.

POST /<path>[?fail=N]  records the webhook; the first N requests to the same path
                       get 503, later ones 200
GET  /<prefix>         webhooks received on paths starting with the prefix
GET  /                 everything received so far
GET  /health           liveness check

Standard library only, keeps everything in memory.
"""

import json
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

received: list[dict[str, Any]] = []
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        url = urlsplit(self.path)
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            body: Any = json.loads(raw)
        except ValueError:
            body = raw.decode(errors="replace")
        try:
            fail = int(parse_qs(url.query).get("fail", ["0"])[0])
        except ValueError:
            fail = 0

        with lock:
            attempt = 1 + sum(1 for r in received if r["path"] == url.path)
            status = 503 if attempt <= fail else 200
            received.append(
                {
                    "path": url.path,
                    "attempt": attempt,
                    "status": status,
                    "received_at": datetime.now(UTC).isoformat(),
                    "timestamp": time.time(),
                    "content_type": self.headers.get("Content-Type"),
                    "body": body,
                }
            )

        summary = (
            f"payment {body.get('payment_id')} {body.get('status')}"
            if isinstance(body, dict)
            else repr(body)[:80]
        )
        print(f"POST {url.path} attempt {attempt} -> {status}: {summary}", flush=True)
        self._reply(status, {"status": status})

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/health":
            self._reply(200, {"status": "ok"})
            return
        with lock:
            matching = [r for r in received if r["path"].startswith(path)]
        self._reply(200, matching)

    def _reply(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # do_POST prints a more useful line


if __name__ == "__main__":
    print("Webhook receiver listening on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
