from functools import wraps
import os
import time
import dill
import sys
import subprocess
from pydantic import BaseModel, ConfigDict
from typing import Dict, Optional, Sequence, Tuple
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


def communicate(
    endpoint: str,
    expected_response_cls: type = Response,
):
    """
    Decorator to send a request to the backend within a context.
    This decorator injects the target URL into the function and handles the response.
    The response is expected to be a requests.Response object.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            response = None
            try:
                url = f"http://{settings.host}:{settings.port}/{endpoint}"

                response = func(*args, **kwargs, url=url)

                if not isinstance(response, requests.Response):
                    raise TypeError(
                        f"Expected a `requests.Response` object, but got {type(response)}"
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
            except requests.exceptions.RequestException as e:
                return Response(
                    status_code=(
                        response.status_code
                        if response
                        else status.HTTP_500_INTERNAL_SERVER_ERROR
                    ),
                    detail=str(e),
                )

        return wrapper

    return decorator


def post_request(request: Optional["Request"] = None, **kwargs):
    """
    Sends a POST request to the specified URL with the given request data.

    Args:
        request (Request, *optional*): The request data to be sent.

    Returns:
        requests.Response: The response from the POST request.
    """
    return requests.post(
        files=(
            {
                "message": (
                    "message.msg",
                    dill.dumps(request, recurse=True),
                )
            }
            if request
            else None
        ),
        timeout=settings.timeout if settings.timeout > 0 else None,
        **kwargs,
    )


def get_request(url: str, **kwargs):
    """
    Sends a GET request to the specified URL with optional parameters.

    Args:
        url (str): The URL to send the GET request to.
        **kwargs: Additional keyword arguments for the request.

    Returns:
        requests.Response: The response from the GET request.
    """
    return requests.get(
        url,
        timeout=settings.timeout if settings.timeout > 0 else None,
        **kwargs,
    )


class Request(Message):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @communicate("create", TaskResponse)
    @staticmethod
    def create(
        tasks: Sequence[AbstractTask],
        extra_paths: Optional[Sequence[str]] = None,
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

        return post_request(  # type: ignore
            Request(tasks=tasks),
            headers=headers,
            **kwargs,
        )

    @communicate("kill", TaskResponse)
    @staticmethod
    def kill(
        name: Optional[str], force: bool = False, use_regex: bool = False, **kwargs
    ) -> TaskResponse:
        """
        Kill specified tasks by name or regex expression. If no name is provided, all tasks will be killed.

        Args:
            name (str, *optional*): The full name of task to kill. If None, kill all tasks.
            force (bool): Whether to force kill the task. Default is False.
            use_regex (bool): Whether to match task names using regex. Default is False.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task killing.
        """
        return post_request(  # type: ignore
            Request(name=name, force=force, use_regex=use_regex), **kwargs
        )

    @communicate("list")
    @staticmethod
    def list(filter: Optional[str], attrs: Tuple[str], **kwargs) -> Response:
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
        return get_request(**kwargs, params={"filter": filter, "attrs": " ".join(attrs)})  # type: ignore

    @communicate("set", Response)
    @staticmethod
    def set_task_attr(name: str, attr: str, value: str, **kwargs) -> Response:
        """
        Set a specific attribute of a task to a new value.

        Args:
            name (str): The name of the task to modify.
            attr (str): The attribute to set.
            value (str): The new value for the attribute.

        Returns:
            Response: A response object containing the status code and details of the task modification.
        """
        return post_request(
            Request(name=name, attr=attr, value=value), **kwargs  # type: ignore
        )

    @communicate("cancel", TaskResponse)
    @staticmethod
    def cancel(name: Optional[str], use_regex: bool = False, **kwargs) -> TaskResponse:
        """
        Cancel tasks by name or regex expression. If no name is provided, all tasks will be cancelled.

        Args:
            name (str, *optional*): The full name of task to cancel. If None, cancel all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task cancellation.
        """
        return post_request(Request(name=name, use_regex=use_regex), **kwargs)  # type: ignore

    @communicate("resume", TaskResponse)
    @staticmethod
    def resume(name: Optional[str], use_regex: bool = False, **kwargs) -> TaskResponse:
        """
        Resume tasks by name or regex expression. If no name is provided, all tasks will be resumed.

        Args:
            name (str, *optional*): The full name of task to resume. If None, resume all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task resumption.
        """
        return post_request(Request(name=name, use_regex=use_regex), **kwargs)  # type: ignore

    @communicate("remove", TaskResponse)
    @staticmethod
    def remove(name: Optional[str], use_regex: bool = False, **kwargs) -> TaskResponse:
        """
        Remove tasks by name or regex expression. If no name is provided, all tasks will be removed.

        Args:
            name (str, *optional*): The full name of task to remove. If None, remove all tasks.
            use_regex (bool): Whether to match task names using regex. Default is False.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            TaskResponse: A response object containing the status code and details of the task removal.
        """
        return post_request(Request(name=name, use_regex=use_regex), **kwargs)  # type: ignore
    
    @communicate("occupy", Response)
    @staticmethod
    def occupy(device_ids: Sequence[int], evil: bool = False, **kwargs) -> Response:
        """
        Occupy specified devices. This will prevent other users' tasks from using the specified devices.

        Args:
            device_ids (list of int): The IDs of the devices to occupy, which is controlled by
                platform-specific environment variables.
            evil (bool): Whether to occupy the devices in an evil way. Default is False.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: A response object containing the status code and details of the occupation.
        """
        return post_request(Request(device_ids=device_ids, evil=evil), **kwargs)  # type: ignore

    @communicate("exit")
    @staticmethod
    def exit(**kwargs) -> Response:
        """
        Exit backend. This will remove all tasks forcefully and exit the backend process.

        Args:
            **kwargs: Additional keyword arguments for the request.

        Returns:
            Response: A response object containing the status code and details of the exit operation.
        """
        return post_request(**kwargs)  # type: ignore

    @communicate("test")
    @staticmethod
    def test(name: Optional[str] = None, **kwargs) -> Response:
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
            return get_request(kwargs["url"])  # type: ignore
        else:
            return get_request(f"{kwargs['url']}/{name}")  # type: ignore


def where() -> str:
    """
    Get cache dirname of current running backend. Raise an error if the backend is not running.
    """
    if os.path.isdir(get_cache_dir("backend")):
        # if where is called in backend, return directly
        return get_cache_dir("")

    response = Request.test()
    if response.error():
        raise RuntimeError(
            f"Failed to query cache dirname of current backend: {response.detail}"
        )

    return get_cache_dir("", response.pid)  # type: ignore


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
