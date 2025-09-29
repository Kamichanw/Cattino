import asyncio
import functools
import os
import subprocess
import sys
import logging

from pathlib import Path
from typing import Optional

from .base import Response, Transmittable, communicate, Request
from cattino import settings

MessageLevel = int


class Message(Transmittable):
    tag: str
    content: str
    level: MessageLevel = logging.INFO

    def __init__(
        self,
        content: str,
        tag: str | None = None,
        level: MessageLevel = logging.INFO,
    ):
        """
        This class packs message data for communication between the client and the mailbox.

        Args:
            tag (str, *optional*): The identifier of the message. If None, defaults to process id.
        """
        super().__init__(tag=tag or str(os.getpid()), content=content, level=level)


class MessageListResponse(Response):
    messages: list[Message] = []


class MsgBoxRequest(Request):
    @staticmethod
    @communicate("msg", port=settings.msgbox_port)
    def info(content: str, tag: str | None = None, **kwargs):
        return Request.post(
            Message(tag=tag, content=content, level=logging.INFO), **kwargs
        )

    @staticmethod
    @communicate("msg", port=settings.msgbox_port)
    def warning(content: str, tag: str | None = None, **kwargs):
        return Request.post(
            Message(tag=tag, content=content, level=logging.WARNING), **kwargs
        )

    @staticmethod
    @communicate("msg", port=settings.msgbox_port)
    def error(content: str, tag: str | None = None, **kwargs):
        return Request.post(
            Message(tag=tag, content=content, level=logging.ERROR), **kwargs
        )

    @staticmethod
    @communicate("msg", port=settings.msgbox_port)
    def critical(content: str, tag: str | None = None, **kwargs):
        return Request.post(
            Message(tag=tag, content=content, level=logging.CRITICAL), **kwargs
        )

    @staticmethod
    @communicate("test", port=settings.msgbox_port)
    def test(**kwargs) -> Response:
        """
        Query the status of the message box.

        Returns:
            Response: A response object containing the status code and an optional PID.
                1. If the target is not running, the status code will be 202.
                2. If the target is running, the status code will be 200 and the PID will be returned (if possible).
        """
        return Request.get(kwargs["url"])  # type: ignore

    @staticmethod
    @communicate("exit", port=settings.msgbox_port)
    def exit(**kwargs) -> Response:
        """
        Exit the message box server.

        Returns:
            Response: A response object containing the status code.
        """
        return Request.post(**kwargs)  # type: ignore

    @staticmethod
    @communicate(
        "fetch", port=settings.msgbox_port, return_response_cls=MessageListResponse
    )
    def fetch(**kwargs) -> MessageListResponse:
        """
        Fetch all messages from the message box. A message will be removed from the box once it is fetched.

        Returns:
            Response: A response object containing the status code and a list of messages.
        """
        return Request.post(**kwargs)  # type: ignore


async def start_msgbox(
    host: Optional[str] = None,
    port: Optional[int] = None,
    backend_dir: Optional[str] = None,
):
    cmd = [
        sys.executable,
        "-u",
        str(Path(__file__).parent.parent / "msgbox.py"),
    ]

    if host:
        cmd.extend(["--host", host])
    if port:
        cmd.extend(["--port", str(port)])
    if backend_dir:
        cmd.extend(["--backend-dir", backend_dir])

    return await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )


def send_msg_on_error(tag: str | None = None):
    """
    A decorator that send an error message to the message box when the decorated async function raises an exception.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                MsgBoxRequest.error(str(e), tag=tag)
                raise

        return wrapper

    return decorator
