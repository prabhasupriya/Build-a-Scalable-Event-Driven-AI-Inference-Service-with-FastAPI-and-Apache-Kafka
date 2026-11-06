"""Minimal asyncio HTTP server exposing ``GET /health`` for the consumer.

The consumer is a long-running worker, not a web app, so instead of pulling in a
web framework this module speaks just enough HTTP/1.1 for Docker / Kubernetes
probes (and ``curl``).
"""
import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Tuple

from app.logging_config import log_event

logger = logging.getLogger(__name__)

HealthProvider = Callable[[], Awaitable[Tuple[bool, Dict[str, Any]]]]

_REASONS = {200: "OK", 404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable"}
_READ_TIMEOUT = 5


def _http_response(status_code: int, body: Dict[str, Any]) -> bytes:
    payload = json.dumps(body).encode("utf-8")
    head = (
        f"HTTP/1.1 {status_code} {_REASONS.get(status_code, 'Error')}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "Connection: close\r\n\r\n"
    )
    return head.encode("ascii") + payload


async def _handle(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, provider: HealthProvider
) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)
        while True:  # drain the request headers
            line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)
            if line in (b"\r\n", b"\n", b""):
                break

        parts = request_line.decode("latin-1", errors="replace").split()
        method = parts[0] if parts else ""
        path = parts[1].split("?")[0] if len(parts) > 1 else ""

        if path != "/health":
            response = _http_response(404, {"detail": "Not found"})
        elif method != "GET":
            response = _http_response(405, {"detail": "Method not allowed"})
        else:
            healthy, details = await provider()
            response = _http_response(200 if healthy else 503, details)

        writer.write(response)
        await writer.drain()
    except (asyncio.TimeoutError, ConnectionError) as exc:
        log_event(logger, logging.DEBUG, "health_connection_error", error=repr(exc))
    except Exception as exc:  # noqa: BLE001 - never let a probe crash the worker
        log_event(logger, logging.ERROR, "health_handler_error", exc_info=True, error=repr(exc))
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass


async def start_health_server(
    provider: HealthProvider, host: str = "0.0.0.0", port: int = 8001  # noqa: S104
) -> asyncio.base_events.Server:
    """Start the health server and return it (caller closes it on shutdown)."""
    server = await asyncio.start_server(lambda r, w: _handle(r, w, provider), host, port)
    log_event(logger, logging.INFO, "health_server_started", host=host, port=port)
    return server
