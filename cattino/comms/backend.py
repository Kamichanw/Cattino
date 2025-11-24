import os
import time
import sys
import subprocess

from typing import Sequence, Tuple
from fastapi import status
from pathlib import Path

from cattino.tasks.interface import AbstractTask
from cattino.utils import get_cache_dir
from cattino.comms.base import Request, Response, Transmittable, communicate


class TaskResponse(Response):
    success: Sequence[str] | None = None
    failure: Sequence[str] | None = None

    def __init__(
        self,
        success: Sequence[str] | None = None,
        failure: Sequence[str] | None = None,
        status_code: int | None = None,
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
            if success and not failure:
                # all tasks are successfully processed
                status_code = status.HTTP_200_OK
            elif failure and not success:
                # all tasks failed to be processed
                status_code = status.HTTP_400_BAD_REQUEST
            elif not success and not failure:
                # no tasks processed
                status_code = status.HTTP_204_NO_CONTENT
            else:
                # combination of success and failure
                status_code = status.HTTP_207_MULTI_STATUS

        super().__init__(
            status_code=status_code, success=success, failure=failure, **kwargs
        )


class BackendRequest(Request):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    @communicate("create", return_response_cls=TaskResponse)
    def create(
        tasks: Sequence[AbstractTask],
        extra_paths: Sequence[str] | None = None,
        **kwargs,
    ) -> TaskResponse:
        """
        Create task.

        Args:
            tasks (sequence of AbstractTask): The tasks to be created.
            extra_paths (sequence of str, *optional*): Extra paths for backend when loading task objects.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task creation.
        """
        if extra_paths:
            headers = {"X-Extra-Path": ",".join(extra_paths)}
        else:
            headers = None

        return Request.post(  # type: ignore
            Transmittable(tasks=tasks),
            headers=headers,
            **kwargs,
        )

    @staticmethod
    @communicate("kill", return_response_cls=TaskResponse)
    def kill(
        name: str | None,
        force: bool = False,
        use_regex: bool = False,
        filter: str | None = None,
        **kwargs,
    ) -> TaskResponse:
        """
        Kill specified tasks by name or regex expression. If no name is provided, all tasks will be killed.

        Args:
            name (str, *optional*): The full name of task to kill. If None, kill all tasks.
            force (bool): Whether to force kill the task. Default is False.
            use_regex (bool): Whether to match task names using regex. Default is False.
            filter (str, *optional*): An optional backend-level filter expression to limit which tasks are affected.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task killing.
        """
        return Request.post(  # type: ignore
            Transmittable(name=name, force=force, use_regex=use_regex, filter=filter),
            **kwargs,
        )

    @staticmethod
    @communicate("list")
    def list(filter: str | None, attrs: Tuple[str, ...], **kwargs) -> Response:
        """
        Query specified attributes of tasks that match the given condition.

        Args:
            filter (str, *optional*): A filter condition to apply to the task query.
            attrs (Tuple[str]): The attributes to retrieve for the matching tasks.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: A response object containing the status code and details of the task query.
                It also contains a field `results` that is a list of dictionaries, each containing the
                specified attributes of a task and its name.
        """
        return Request.get(**kwargs, params={"filter": filter, "attrs": " ".join(attrs)})  # type: ignore

    @staticmethod
    @communicate("set")
    def set_task_attr(
        name: str | None,
        attr: str,
        value: str,
        use_regex: bool = False,
        filter: str | None = None,
        **kwargs,
    ) -> Response:
        """
        Set a specific attribute of a task to a new value.

        Args:
            name (str): The name of the task to modify.
            attr (str): The attribute to set.
            value (str): The new value for the attribute.
            use_regex (bool): Whether to match task names using regex. Default is False.
            filter (str, *optional*): An optional backend-level filter expression to limit which tasks are affected.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: A response object containing the status code and details of the task modification.
        """
        return Request.post(
            Transmittable(name=name, attr=attr, value=value, use_regex=use_regex, filter=filter), **kwargs  # type: ignore
        )

    @staticmethod
    @communicate("cancel", return_response_cls=TaskResponse)
    def cancel(
        name: str | None, use_regex: bool = False, filter: str | None = None, **kwargs
    ) -> TaskResponse:
        """
        Cancel tasks by name or regex expression. If no name is provided, all tasks will be cancelled.

        Args:
            name (str, *optional*): The full name of task to cancel. If None, cancel all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            filter (str, *optional*): An optional backend-level filter expression to limit which tasks are affected.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task cancellation.
        """
        return Request.post(Transmittable(name=name, use_regex=use_regex, filter=filter), **kwargs)  # type: ignore

    @staticmethod
    @communicate("resume", return_response_cls=TaskResponse)
    def resume(
        name: str | None, use_regex: bool = False, filter: str | None = None, **kwargs
    ) -> TaskResponse:
        """
        Resume tasks by name or regex expression. If no name is provided, all tasks will be resumed.

        Args:
            name (str, *optional*): The full name of task to resume. If None, resume all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            filter (str, *optional*): An optional backend-level filter expression to limit which tasks are affected.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task resumption.
        """
        return Request.post(Transmittable(name=name, use_regex=use_regex, filter=filter), **kwargs)  # type: ignore

    @staticmethod
    @communicate("remove", return_response_cls=TaskResponse)
    def remove(
        name: str | None, use_regex: bool = False, filter: str | None = None, **kwargs
    ) -> TaskResponse:
        """
        Remove tasks by name or regex expression. If no name is provided, all tasks will be removed.

        Args:
            name (str, *optional*): The full name of task to remove. If None, remove all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            filter (str, *optional*): An optional backend-level filter expression to limit which tasks are affected.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task removal.
        """
        return Request.post(Transmittable(name=name, use_regex=use_regex, filter=filter), **kwargs)  # type: ignore

    @staticmethod
    @communicate("exit")
    def exit(**kwargs) -> Response:
        """
        Exit backend. This will remove all tasks forcefully and exit the backend process.

        Args:
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: A response object containing the status code and details of the exit operation.
        """
        return Request.post(**kwargs)  # type: ignore

    @staticmethod
    @communicate("test")
    def test(name: str | None = None, **kwargs) -> Response:
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
        if name is None or name == "backend":
            return Request.get(kwargs["url"])  # type: ignore
        else:
            return Request.get(f"{kwargs['url']}/{name}")  # type: ignore


def where() -> str:
    """
    Get cache dirname of current running backend. Raise an error if the backend is not running.
    """
    if os.path.isdir(get_cache_dir("backend")):
        # if where is called in backend, return directly
        return get_cache_dir()

    response = BackendRequest.test()
    if response.error():
        raise RuntimeError(
            f"Failed to query cache dirname of current backend: {response.detail}"
        )

    return get_cache_dir("", response.pid)  # type: ignore


def start_backend(
    blocking: bool = False, host: str | None = None, port: int | None = None
):
    cmd = [
        sys.executable,
        "-u",
        str(Path(__file__).parent.parent / "backend.py"),
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
        elapsed = 0
        interval = 3
        while not (response := BackendRequest.test()).ok():
            time.sleep(interval)
            elapsed += interval
            print(
                f"{response.status_code}: {response.detail} Waiting for backend to start... {elapsed}s",
                flush=True,
            )
