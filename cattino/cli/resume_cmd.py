import click
import sys

from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import (
    fetch_from_msgbox,
    print_response,
    print_confirm,
    MagicString,
)
from cattino.cli.console import console


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Resume all tasks.",
)
@click.option(
    "--yes",
    "-y",
    "yes",
    is_flag=True,
    default=False,
    help="Automatically confirm operations and proceed without asking the user.",
)
@click.option(
    "--filter",
    "filter_",
    type=MagicString(),
    required=False,
    help="Filter tasks by a python expression that accept a argument 'task'.",
)
@click.option(
    "--regex",
    "-r",
    "use_regex",
    is_flag=True,
    default=False,
    help="Interpret the provided name as a regular expression.",
)
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def resume(
    all: bool, use_regex: bool, filter_: str | None, name: str | None, yes: bool
):
    """
    Resume cancelled or failed tasks.

    Resume tasks that are in `Cancelled` or `Failed` states. Targets can be
    specified by `name`, selected with `-A/--all` (select all tasks), matched
    with `-r/--regex` (interpret `name` as a regex), or selected with
    `--filter` (Python expression accepting `task`).

    \b
    Examples:
      meow resume mytask
      meow resume --filter "task.priority >= 10"
    """

    if name and all:
        console.print("Options -A/--all and NAME cannot be used together.")
        sys.exit(1)

    if not name and not all and not filter_ and not use_regex:
        console.print("No task name provided.")
        sys.exit(1)

    if not yes:
        if name:
            print_confirm(name, use_regex, filter_)
        else:
            click.confirm("Are you sure you want to resume all tasks?", abort=True)

    response = BackendRequest.resume(name, use_regex=use_regex, filter=filter_)
    if response.error():
        console.print(response.detail)
        sys.exit(1)

    if yes:
        print_response(response)
