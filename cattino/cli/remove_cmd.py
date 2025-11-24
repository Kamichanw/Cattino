import click
import sys
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import (
    fetch_from_msgbox,
    print_response,
    MagicString,
)
from cattino.cli.console import console


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Remove all tasks.",
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
def remove(all: bool, use_regex: bool, filter_: str | None, name: str | None):
    """
    Remove tasks from the scheduler.

    Remove one or multiple tasks. Specify a `name`, use `-A/--all` to select all
    tasks, use `-r/--regex` to interpret the provided name as a regular
    expression, or use `--filter` to select tasks via a Python expression that
    receives a `task` object. The backend (task named `backend`) cannot be removed.

    \b
    Examples:
      meow remove mytask
      meow remove --filter "task.name.startswith('temp')"
    """

    if name and "backend" in name:
        console.print("Backend cannot be removed.")
        sys.exit(1)

    if name and all:
        console.print("Options -A/--all and NAME cannot be used together.")
        sys.exit(1)

    if not name and not all and not filter_ and not use_regex:
        console.print("No task name provided.")
        sys.exit(1)

    response = BackendRequest.remove(name, use_regex=use_regex, filter=filter_)
    print_response(response)
    if response.error():
        sys.exit(1)
