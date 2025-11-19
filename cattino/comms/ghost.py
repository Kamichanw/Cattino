from typing import Optional
import asyncio
import sys
from pathlib import Path

from cattino.comms.base import Transmittable, Response


class GhostCommand(Transmittable):
    device: int | None = None
    mem_bytes: int | None = None
    compute_on_idle: bool | None = False


class GhostStatusResponse(Response):
    results: dict | None = None


async def start_ghost(host: str | None = None, port: int | None = None):
    """Spawn a ghost process (async) and return the Process handle with pipes.

    This mirrors `start_msgbox` so the backend can create a managed ghost subprocess
    and read its stdout/stderr when not redirecting output.
    """
    cmd = [
        sys.executable,
        "-u",
        str(Path(__file__).parent.parent / "ghost.py"),
    ]
    if host:
        cmd.extend(["--host", host])
    if port:
        cmd.extend(["--port", str(port)])

    # Start the ghost process without piping stdout/stderr. The ghost
    # service should send notifications via the message box RPCs instead
    # of relying on stdout/stderr capture by the backend.
    return await asyncio.create_subprocess_exec(*cmd)
