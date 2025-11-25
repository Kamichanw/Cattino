import sys
import rich
import logging
import click
import gettext

from functools import wraps
from datetime import datetime
from rich.logging import RichHandler
from rich.tree import Tree as RichTree
from rich.text import Text
from typing import Sequence, Any, Literal

from Cattino.cattino.comms.backend import BackendRequest
from cattino.comms import Response, MsgBoxRequest
from cattino.core.path_tree import PathTree
from cattino.utils import Magics
from cattino.cli.console import console


def print_response(response: Response):
    """
    Build and print a rich.Tree that shows per-task status with icons and colors.
    """
    success = list(getattr(response, "success", []) or [])
    failure = list(getattr(response, "failure", []) or [])
    no_op = list(getattr(response, "no_op", []) or [])
    exception = getattr(response, "exception", None)

    # build mapping from failure fullname -> exception message when exception is list
    exception_map: dict[str, str] = (
        {name: str(msg) for name, msg in zip(failure, exception)}
        if isinstance(exception, list)
        else {}
    )

    ordered = []
    for name_list, status in (
        (success, "success"),
        (failure, "failure"),
        (no_op, "no_op"),
    ):
        ordered.extend((n, status) for n in name_list)

    tree = PathTree(sep="/")
    for fullname, status in ordered:
        msg = None
        if status == "failure" and fullname in exception_map:
            msg = exception_map[fullname]
        tree.set_node(fullname, (status, msg))

    root = RichTree(Text("Tasks", style="bold"), hide_root=True)

    ICONS = {"success": "✔", "failure": "✖", "no_op": "○"}
    COLORS = {"success": "green", "failure": "red", "no_op": "white"}

    def render_node(parent_branch, node):
        if node.children:
            branch = parent_branch.add(Text(node.name))
            for child in node.children.values():
                render_node(branch, child)
        else:
            status, msg = node.data
            t = Text()
            t.append(
                f"{ICONS[status]} {node.name}",
                style=("white" if isinstance(exception, str) else COLORS[status]),
            )
            if msg:
                t.append(" – ")
                t.append(str(msg), style="red")
            parent_branch.add(t)

    # if tree has no roots (no names), nothing to show
    if tree.roots:
        for node in tree.roots.values():
            render_node(root, node)
        console.print(root)

    # if exception is a plain string, print it after the tree
    if isinstance(exception, str):
        console.print(Text(str(exception), style="red"))

def print_confirm(name: str, use_regex: bool, filter_: str | None):
    """
    Print the confirmation message in tree format.
    """
    response = BackendRequest.list(name, use_regex=use_regex, filter=filter_, attrs=("fullname",))
    if response.error():
        console.print(response.detail)
        sys.exit(1)
    if not (results := response.results): # type: ignore
        console.print("No tasks found.")
        sys.exit(0)
    
    tree = PathTree(sep="/")
    for result in results:
        tree.set_node(result["fullname"], None)

    root = RichTree(Text("Tasks", style="bold"), hide_root=True)
    def render_node(parent_branch, node):
        if node.children:
            branch = parent_branch.add(Text(node.name))
            for child in node.children.values():
                render_node(branch, child)
        else:
            parent_branch.add(Text(node.name))
    
    for node in tree.roots.values():
        render_node(root, node)
        
    console.print(root)
    click.confirm(
        "[bold red]Are you sure you want to proceed with the selected tasks?[/bold red]",
        abort=True,
    )
        

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
