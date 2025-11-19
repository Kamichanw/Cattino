import asyncio
import builtins
import logging
import re
import click
import dill
import os
import signal
import sys
import fastapi
import uvicorn
from contextlib import asynccontextmanager
from typing import Callable, List, Optional
from collections.abc import Sequence
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from loguru import logger

from cattino.comms.backend import BackendRequest, Response, TaskResponse
from cattino.core.task_scheduler import TaskScheduler
from cattino.tasks.interface import Task, TaskGroup, TaskStatus
from cattino.settings import settings
from cattino.utils import (
    Magics,
    get_cache_dir,
    get_cattino_home,
    has_param_type,
    open_redirected_stream,
    setup_logger,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ghost_manager = GhostManager(device_ids=settings.visible_devices)
    yield



app = FastAPI(lifespan=lifespan)


class GhostManager:
    """A minimal in-memory ghost manager scaffold.

    This manager only stores requested state for ghosts per-device. It is
    intentionally lightweight so it can be integrated with the device
    allocator later. Actual device memory reservation is not performed here;
    this provides the control surface and basic state for the comms API.
    """

    def __init__(self, device_ids: list[int] | None = None):
        self._devices = {}
        if device_ids is None:
            device_ids = []
        for d in device_ids:
            self._devices[int(d)] = {
                "active": False,
                "mem_target": None,
                "is_computing": False,
            }

    def _ensure_device(self, device: int):
        if device not in self._devices:
            self._devices[int(device)] = {
                "active": False,
                "mem_target": None,
                "is_computing": False,
            }

    def start(self, device: int | None = None, mem_bytes: int | None = None, compute_on_idle: bool = False):
        devices = [device] if device is not None else list(self._devices.keys())
        for d in devices:
            self._ensure_device(d)
            self._devices[int(d)]["active"] = True
            if mem_bytes is not None:
                self._devices[int(d)]["mem_target"] = int(mem_bytes)
            self._devices[int(d)]["is_computing"] = bool(compute_on_idle)

    def stop(self, device: int | None = None):
        devices = [device] if device is not None else list(self._devices.keys())
        for d in devices:
            self._ensure_device(d)
            self._devices[int(d)]["active"] = False
            self._devices[int(d)]["is_computing"] = False

    def set_mem(self, device: int | None = None, mem_bytes: int | None = None):
        devices = [device] if device is not None else list(self._devices.keys())
        for d in devices:
            self._ensure_device(d)
            self._devices[int(d)]["mem_target"] = int(mem_bytes) if mem_bytes is not None else None

    def status(self, device: int | None = None):
        if device is None:
            return {str(k): v.copy() for k, v in self._devices.items()}
        self._ensure_device(device)
        return {str(device): self._devices[int(device)].copy()}


def load_backend_request(message: UploadFile = File(...), request: fastapi.Request = None):  # type: ignore
    """
    Load the request from the uploaded file. This mirrors the implementation used in
    the main backend so comms wrappers that post dill-packed messages work.
    """
    msg = message.file.read()
    old_path = sys.path
    # add extra paths to load user-defined modules
    if extra_modules := request.headers.get("X-Extra-Path", None):
        sys.path = extra_modules.split(",") + sys.path
    try:
        request = dill.loads(msg)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load request: {e}",
        )
    sys.path = old_path
    return request


@app.post("/ghost/start", response_model=Response)
async def ghost_start(data: BackendRequest = Depends(load_backend_request)):
    """Start ghost on a device or on all known devices.

    Accepts fields: `device` (optional int), `mem_bytes` (optional int),
    `compute_on_idle` (optional bool).
    """
    gm: GhostManager = app.state.ghost_manager
    device = getattr(data, "device", None)
    mem = getattr(data, "mem_bytes", None)
    compute = getattr(data, "compute_on_idle", False)
    gm.start(device=device, mem_bytes=mem, compute_on_idle=compute)
    return Response(status_code=status.HTTP_200_OK)


@app.post("/ghost/stop", response_model=Response)
async def ghost_stop(data: BackendRequest = Depends(load_backend_request)):
    device = getattr(data, "device", None)
    gm: GhostManager = app.state.ghost_manager
    gm.stop(device=device)
    return Response(status_code=status.HTTP_200_OK)


@app.post("/ghost/set", response_model=Response)
async def ghost_set(data: BackendRequest = Depends(load_backend_request)):
    device = getattr(data, "device", None)
    mem = getattr(data, "mem_bytes", None)
    gm: GhostManager = app.state.ghost_manager
    gm.set_mem(device=device, mem_bytes=mem)
    return Response(status_code=status.HTTP_200_OK)


@app.post("/ghost/status", response_model=Response)
async def ghost_status(data: BackendRequest = Depends(load_backend_request)):
    device = getattr(data, "device", None)
    gm: GhostManager = app.state.ghost_manager
    return Response(status_code=status.HTTP_200_OK, results=gm.status(device))


@click.command()
@click.option(
    "--host",
    type=str,
    required=False,
    help="Host to run the backend server on.",
)
@click.option(
    "--port",
    type=int,
    required=False,
    help="Port to run the backend server on.",
)
def run(host: str | None, port: int | None):
    uvicorn.run(
        app,
        host=host or settings.host,
        port=port or settings.port,
        access_log=settings.debugging,
        log_config=None,
    )


if __name__ == "__main__":
    run()
