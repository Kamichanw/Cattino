import rich
import logging
import click
import gettext

from functools import wraps
from datetime import datetime
from rich.logging import RichHandler
from typing import Callable, Sequence, Any, Literal

from cattino.comms import Response, MsgBoxRequest
from cattino.core.path_tree import PathTree
from cattino.utils import Magics
from cattino.cli.console import console


def print_response(
    response: Response,
    success_msg_fn: Callable[[list[str] | None], str | None],
    failure_msg_fn: Callable[[list[str] | None], str | None],
):
    """
    Print the response from the backend. It takes a response object and
    success, failure, and optional no-op message producers. These producers optionally
    take a list of task names and returns a message to be printed.

    For success_msg_fn, it should additionally handle the case when the response.ok() is True.
    """
    success_msg = success_msg_fn(getattr(response, "success", None))
    failure_msg = failure_msg_fn(getattr(response, "failure", None))

    def echo(msg: str | None):
        if msg:
            console.print(msg)

    echo(success_msg)
    echo(failure_msg)


def get_path_tree_str(names: list[str]):
    """
    Convert a list of names to a path tree string, which collapses common prefixes and
    groups siblings with curly braces.

    Examples:
        >>> get_path_tree_str(['a/b/c', 'a/b/d', 'a/e', 'f'])
        "a/{b/{c, d}, e}, f"
    """
    tree = PathTree()
    for name in names:
        tree.set_node(name, None)
    return str(tree)


logger = logging.getLogger("mailbox_logger")
logger.setLevel(logging.INFO)
logger.addHandler(
    RichHandler(
        level="INFO",
        console=console,
        show_time=True,
        show_level=True,
        show_path=False,
        markup=True,
        keywords=None,
        log_time_format="%H:%M:%S",
    )
)
logger.propagate = False


def fetch_from_msgbox(func):
    """
    Fetch messages from the message box and log them.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        response = MsgBoxRequest.fetch()
        if hasattr(response, "messages"):
            for message in response.messages:
                logger.log(
                    message.level,
                    (
                        f"[bold magenta]{message.tag}[/bold magenta]: {message.content}"
                        if message.tag
                        else message.content
                    ),
                )

        return func(*args, **kwargs)

    return wrapper


class MagicString(click.ParamType):
    name = "magic_string"

    def convert(self, value, param, ctx):
        if not isinstance(value, str):
            self.fail(f"{value} is not a valid string", param, ctx)

        return Magics.resolve(value)


class RequiresMemo(click.ParamType):
    name = "requires_memory"

    def convert(self, value, param, ctx):
        try:
            value = float(value) if "." in str(value) else int(value)
        except (TypeError, ValueError):
            self.fail(f"{value} is not a valid integer or float", param, ctx)
        if value < 0:
            self.fail(
                f"{value} is not a valid non-negative integer or float", param, ctx
            )
        if isinstance(value, float):
            if not (0 <= value <= 1):
                self.fail(f"{value} is not a valid float between 0 and 1", param, ctx)
        return value


class DateTime(click.DateTime):
    def __init__(
        self,
        formats: Sequence[str] | None = None,
        fill_default: Literal["latest", "earliest"] = "earliest",
        **kwargs,
    ):
        super().__init__(
            formats=formats
            or [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d %H",
                "%Y-%m-%d",
                "%Y-%m",
                "%Y",
            ],
            **kwargs,
        )
        assert all(
            fmt
            for fmt in self.formats
            if fmt
            in [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d %H",
                "%Y-%m-%dT%H",
                "%Y-%m-%d",
                "%Y-%m",
                "%Y",
            ]
        )
        self.fill_default = fill_default

    def convert(
        self, value: str, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        if self.fill_default == "earliest":
            return super().convert(value, param, ctx)
        if isinstance(value, datetime):
            return value

        for fmt in self.formats:
            try:
                date_obj = datetime.strptime(value, fmt)
                if fmt == "%Y":
                    return date_obj.replace(date_obj.year, 12, 31, 23, 59, 59)
                if fmt == "%Y-%m":
                    return date_obj.replace(
                        date_obj.year, date_obj.month, 31, 23, 59, 59
                    )
                if fmt == "%Y-%m-%d":
                    return date_obj.replace(
                        date_obj.year, date_obj.month, date_obj.day, 23, 59, 59
                    )
                if fmt in ["%Y-%m-%d %H", "%Y-%m-%dT%H"]:
                    return date_obj.replace(
                        date_obj.year,
                        date_obj.month,
                        date_obj.day,
                        date_obj.hour,
                        59,
                        59,
                    )
                if fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"]:
                    return date_obj.replace(
                        date_obj.year,
                        date_obj.month,
                        date_obj.day,
                        date_obj.hour,
                        date_obj.minute,
                        59,
                    )
                return date_obj

            except ValueError:
                continue

        formats_str = ", ".join(map(repr, self.formats))
        self.fail(
            gettext.ngettext(
                "{value!r} does not match the format {format}.",
                "{value!r} does not match the formats {formats}.",
                len(self.formats),
            ).format(value=value, format=formats_str, formats=formats_str),
            param,
            ctx,
        )
