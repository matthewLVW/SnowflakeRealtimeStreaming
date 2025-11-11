#!/usr/bin/env python3
"""FastAPI-based live viewer that proxies mock API ticks over websockets."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

ROOT_DIR = Path(__file__).resolve().parent
STATIC_DIR = ROOT_DIR / "static"

DEFAULT_UPSTREAM =  "http://127.0.0.1:8000/ticks"
RECONNECT_DELAY_S = float(os.environ.get("VIEWER_RECONNECT_SEC", "2.0"))


class ConnectionManager:
    """Tracks active websocket connections and handles broadcast delivery."""

    def __init__(self) -> None:
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._has_connections = asyncio.Event()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            self._has_connections.set()

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)
            if not self._connections:
                self._has_connections.clear()

    async def broadcast(self, payload: Dict[str, object]) -> None:
        async with self._lock:
            connections = list(self._connections)
        if not connections:
            return
        message = json.dumps(payload, separators=(",", ":"))
        stale: List[WebSocket] = []
        for websocket in connections:
            try:
                await websocket.send_text(message)
            except Exception:
                stale.append(websocket)
        if stale:
            async with self._lock:
                for websocket in stale:
                    self._connections.discard(websocket)
                if not self._connections:
                    self._has_connections.clear()

    def has_connections(self) -> bool:
        return self._has_connections.is_set()

    async def wait_for_connection(self) -> None:
        await self._has_connections.wait()

    def connection_count(self) -> int:
        return len(self._connections)


class TickStreamer:
    """Background task that tails the upstream mock API and fan-outs events."""

    def __init__(self, manager: ConnectionManager, upstream_url: str) -> None:
        self._manager = manager
        self._upstream_url = upstream_url
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._client: Optional[httpx.AsyncClient] = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            timeout = httpx.Timeout(connect=5.0, read=None, write=None, pool=None)
            self._client = httpx.AsyncClient(timeout=timeout, headers={"accept": "application/x-ndjson"})
        return self._client

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            if not self._manager.has_connections():
                try:
                    await asyncio.wait_for(self._manager.wait_for_connection(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
            try:
                client = await self._ensure_client()
                async with client.stream("GET", self._upstream_url) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if self._stop_event.is_set():
                            return
                        if not line:
                            continue
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        await self._manager.broadcast(event)
                        if not self._manager.has_connections():
                            break
            except (httpx.HTTPError, asyncio.CancelledError):
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(RECONNECT_DELAY_S)
            except Exception:
                # Unexpected failure; pause briefly before retrying.
                await asyncio.sleep(RECONNECT_DELAY_S)


app = FastAPI(title="Tick Stream Viewer")
manager = ConnectionManager()
streamer = TickStreamer(manager, DEFAULT_UPSTREAM)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def _on_startup() -> None:
    streamer.start()


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    await streamer.stop()


def _load_index_html() -> str:
    index_path = STATIC_DIR / "live_viewer.html"
    if not index_path.exists():
        raise FileNotFoundError(f"Static viewer not found: {index_path}")
    return index_path.read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return _load_index_html()


@app.get("/health")
async def health() -> Dict[str, object]:
    return {
        "status": "ok",
        "upstream_url": DEFAULT_UPSTREAM,
        "clients": manager.connection_count(),
    }


@app.websocket("/ws/ticks")
async def websocket_ticks(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception:
        await manager.disconnect(websocket)
