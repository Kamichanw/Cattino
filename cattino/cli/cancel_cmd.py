import click
import sys

from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import MagicString, fetch_from_msgbox, print_confirm, print_response
from cattino.cli.console import console


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Cacnel all tasks.",
)
@click.option(
    "--regex",
    "-r",
    "use_regex",
    is_flag=True,
    default=False,
    help="Interpret the provided name as a regular expression.",
)
@click.option(
    "--filter",
    "filter_",
    type=MagicString(),
    required=False,
    help="Filter tasks by a python expression that accept a argument 'task'.",
)
@click.option(
    "--yes",
    "-y",
    "yes",
    is_flag=True,
    default=False,
    help="Automatically confirm operations and proceed without asking the user.",
)
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def cancel(all: bool, use_regex: bool, filter_: str | None, name: str | None, yes: bool):
    """
    Cancel running or waiting tasks.

    Cancel tasks that are currently running or queued. Specify a task `name`,
    use `-A/--all` to select all tasks, or use `-r/--regex` to interpret the
    provided name as a regular expression. The command returns which tasks
    were cancelled and which failed.

    Examples:
      meow cancel mytask
      meow cancel -A "^group/.*"
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
            click.confirm(
                "Are you sure you want to cancel all tasks?", abort=True
            )

    response = BackendRequest.cancel(name, use_regex=use_regex, filter=filter_)
    if response.error():
        console.print(response.detail)
        sys.exit(1)
    if yes:
        print_response(response)
