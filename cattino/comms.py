import os
import time
import dill
import sys
import subprocess
from pydantic import BaseModel, ConfigDict
from typing import Dict, Optional, Sequence
import requests
from fastapi import status

from cattino import settings
from cattino.tasks.interface import AbstractTask
from cattino.utils import get_cache_dir


class Message(BaseModel):

    model_config = ConfigDict(extra="allow")

    """
    The message class that is used to pack the command and data into a message that can be used to communicate between
    the CLI and the backend.
    """

    def __init__(self, **kwargs):
        """
        Pack the arguments into a message.

        Args:
            **kwargs: Additional keyword arguments for the command. These arguments will be
                added as attributes of the message object.
        """
        super().__init__(**kwargs)


def send_request(
    endpoint: str,
    expected_response_cls: type,
    request: Optional["Request"] = None,
    api: str = "post",
    headers: Optional[Dict[str, str]] = None,
):
    """
    Post a request to the backend.
    """
    try:
        send_fn = getattr(requests.api, api, None)
        url = f"http://{settings.host}:{settings.port}/{endpoint}"
        if send_fn is None:
            raise ValueError(f"Invalid API method: {api}")
        response: requests.Response = send_fn(
            url,
            files=(
                {"message": ("message.msg", dill.dumps(request, recurse=True))}
                if request
                else None
            ),
            headers=headers,
            timeout=settings.timeout if settings.timeout > 0 else None,
        )
        response_json: dict = response.json()
        response_json.setdefault("status_code", response.status_code)
        return expected_response_cls(**response_json)
    except requests.exceptions.ConnectionError:
        return Response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Backend is not running.",
        )
    except requests.exceptions.Timeout:
        return Response(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Backend is not responding. This may be due to an internal error. Please check the "
            "backend logs for details.",
        )
    except Exception as e:
        return Response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


class Request(Message):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def create(
        tasks: Sequence[AbstractTask],
        extra_paths: Optional[Sequence[str]] = None,
    ):
        """Create a message for creating tasks."""
        if extra_paths:
            headers = {"X-Extra-Path": ",".join(extra_paths)}
        else:
            headers = None
        return send_request(
            "create", TaskResponse, Request(tasks=tasks), headers=headers
        )

    @staticmethod
    def kill(
        name: Optional[str],
        force: bool = False,
        use_regex: bool = False,
    ):
        """
        Kill task.

        Args:
            name (str, *optional*): The full name of task to kill. If None, kill all tasks.
            force (bool): Whether to force kill the task. Default is False.
            use_regex (bool): Whether to match task names using regex. Default is False.
        """
        return send_request(
            "kill",
            TaskResponse,
            Request(name=name, force=force, use_regex=use_regex),
        )

    @staticmethod
    def suspend(name: Optional[str], use_regex: bool = False):
        """Suspend task"""
        return send_request(
            "suspend",
            TaskResponse,
            Request(name=name, use_regex=use_regex),
        )

    @staticmethod
    def resume(name: Optional[str], use_regex: bool = False):
        """Resume task"""
        return send_request(
            "resume",
            TaskResponse,
            Request(name=name, use_regex=use_regex),
        )

    @staticmethod
    def remove(name: Optional[str], use_regex: bool = False):
        """Remove task"""
        return send_request(
            "remove",
            TaskResponse,
            Request(name=name, use_regex=use_regex),
        )

    @staticmethod
    def exit():
        """Exit backend"""
        return send_request("exit", Response)

    @staticmethod
    def status():
        """Get backend status"""
        return send_request("status", Response)

    @staticmethod
    def monitor():
        """Monitor backend"""
        return send_request("monitor", Response)

    @staticmethod
    def test(name: Optional[str] = None):
        """
        Query the backend or a specific task whether it is running.

        Args:
            name (str, *optional*): The name of the task to query. If None or "backend", it will query the backend.

        Returns:
            Response: A response object containing the status code and an optional PID.
                1. If no target is found, the status code will be 404.
                2. If the target is not running, the status code will be 202.
                3. If the target is running, the status code will be 200 and the PID will be returned (if possible).
        """
        return send_request("test", Response, Request(name=name))


class Response(Message):
    """
    Once requests are processed by the backend, the backend will send a response message back to the CLI.
    """

    status_code: int
    detail: Optional[str] = None

    def __init__(self, status_code: int, **kwargs):
        super().__init__(status_code=status_code, **kwargs)

    def __bool__(self):
        """
        Check if the status code of the response is successful.

        Returns:
            bool: True if the response is successful, False otherwise.
        """
        return self.status_code < 400

    def ok(self):
        return self.status_code == status.HTTP_200_OK

    def fail(self):
        return 400 <= self.status_code < 500

    def error(self):
        return self.status_code >= 500


class TaskResponse(Response):
    success: Optional[Sequence[str]] = None
    no_op: Optional[Sequence[str]] = None
    failure: Optional[Sequence[str]] = None

    def __init__(
        self,
        success: Optional[Sequence[str]] = None,
        no_op: Optional[Sequence[str]] = None,
        failure: Optional[Sequence[str]] = None,
        status_code: Optional[int] = None,
        **kwargs,
    ):
        """
        Create a response message for the task execution.

        Args:
            success (sequence of str, *optional*): The names of the tasks that have been successfully processed.
            no_op (sequence of str, *optional*): The names of the tasks that have been processed but no operation is performed.
            failure (sequence of str, *optional*): The names of the tasks that have failed to be processed.
            status_code (int, *optional*): The HTTP status code for the response. If None, it will be set based on
                the success, no_op and failure task list.
            **kwargs: Additional keyword arguments for the message.
        """
        if status_code is None:
            if success and not no_op and not failure:
                # all tasks are successfully processed
                status_code = status.HTTP_200_OK
            elif no_op and not success and not failure:
                # all tasks don't need to be processed
                status_code = status.HTTP_204_NO_CONTENT
            elif failure and not success and not no_op:
                # all tasks failed to be processed
                status_code = status.HTTP_400_BAD_REQUEST
            else:
                # combination of success, no_op or failure
                if not success:
                    # no tasks are successfully processed
                    status_code = status.HTTP_400_BAD_REQUEST
                else:
                    status_code = status.HTTP_207_MULTI_STATUS

        super().__init__(
            status_code=status_code,
            success=success,
            no_op=no_op,
            failure=failure,
            **kwargs,
        )


def where() -> Optional[str]:
    """
    Get cache dirname of current running backend. If the backend is not runnning,
    return None.
    """
    response = Request.test()
    if response.error():
        raise RuntimeError(
            f"Failed to query cache dirname of current backend: {response.detail}"
        )

    return get_cache_dir("", response.pid) if getattr(response, "pid", None) else None # type: ignore


def start_backend(
    blocking: bool = False, host: Optional[str] = None, port: Optional[int] = None
):
    cmd = [
        sys.executable,
        "-u",
        os.path.join(os.path.dirname(__file__), "backend.py"),
    ]
    if not blocking:
        cmd.append("--redirect-output")
    if host:
        cmd.extend(["--host", host])
    if port:
        cmd.extend(["--port", str(port)])

    proc = subprocess.Popen(cmd)

    if blocking:
        proc.wait()
    else:
        time.sleep(3)
