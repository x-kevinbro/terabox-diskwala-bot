"""Tiny HTTP server so Koyeb health checks and UptimeRobot have an endpoint."""
from __future__ import annotations

import time

from aiohttp import web

from config import config

STARTED = time.time()
STATE: dict[str, object] = {"bot": "starting", "active_jobs": 0, "queued_jobs": 0, "pending_batches": 0}


async def _health(_request: web.Request) -> web.Response:
    worker = STATE.get("archive_worker")
    ready = str(STATE.get("bot", "")).startswith("@") and (
        not config.archive_enabled or (STATE.get("archive") == "ready" and worker is not None and not worker.done()))
    return web.json_response(
        {
            "success": ready,
            "status": "ok" if ready else "not_ready",
            "engine": "mtproto",
            "build": "turso-archive-v1",
            "features": ["graph-page-batches", "sequential-delivery", "cancel-batch"] + (["turso-archive", "delivery-bot"] if config.archive_enabled else []),
            "bot": STATE.get("bot"),
            "archive": STATE.get("archive", "disabled"),
            "delivery_bot": STATE.get("delivery_bot"),
            "active_jobs": STATE.get("active_jobs"),
            "queued_jobs": STATE.get("queued_jobs"),
            "pending_batches": STATE.get("pending_batches"),
            "max_file_mb": config.max_file_mb,
            "uptime_seconds": int(time.time() - STARTED),
            "providers": ["diskwala", "terabox"],
        },
        status=200 if ready else 503,
    )


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/api/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.port).start()
    return runner
