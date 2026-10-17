"""Tiny HTTP server so Koyeb health checks and UptimeRobot have an endpoint."""
from __future__ import annotations

import time

from aiohttp import web

from config import config

STARTED = time.time()
STATE: dict[str, object] = {"bot": "starting", "active_jobs": 0, "queued_jobs": 0, "pending_batches": 0}


async def _health(_request: web.Request) -> web.Response:
    return web.json_response(
        {
            "success": True,
            "status": "ok",
            "engine": "mtproto",
            "build": "graph-batch-v1",
            "features": ["graph-page-batches", "sequential-delivery", "cancel-batch"],
            "bot": STATE.get("bot"),
            "active_jobs": STATE.get("active_jobs"),
            "queued_jobs": STATE.get("queued_jobs"),
            "pending_batches": STATE.get("pending_batches"),
            "max_file_mb": config.max_file_mb,
            "uptime_seconds": int(time.time() - STARTED),
            "providers": ["diskwala", "terabox"],
        }
    )


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/api/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.port).start()
    return runner
