import requests
import dill


from functools import wraps
from pydantic import BaseModel, ConfigDict
from typing import Optional, Sequence
from fastapi import status

from cattino import settings


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


class Request(Message):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def post(cls, request: Optional["Request"] = None, **kwargs):
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
            timeout=kwargs.pop(
                "timeout", settings.timeout if settings.timeout > 0 else None
            ),
            **kwargs,
        )

    @classmethod
    def get(cls, url: str, **kwargs):
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
            timeout=kwargs.pop(
                "timeout", settings.timeout if settings.timeout > 0 else None
            ),
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
