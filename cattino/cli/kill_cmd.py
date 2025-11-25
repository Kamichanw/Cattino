import click
import sys
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import (
    fetch_from_msgbox,
    print_confirm,
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
    help="Kill all tasks.",
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
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force kill tasks.",
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
def kill(all: bool, use_regex: bool, force: bool, filter_: str | None, yes: bool, name: str | None):
    """
    Kill running tasks.

    Terminate one or more tasks. Specify a task `name`, use `-A/--all` to select
    all tasks, use `-r/--regex` to interpret the provided name as a regular
    expression, or provide `--filter` with a Python expression that
    receives a `task` object to select targets. Use `-f/--force` to force
    termination.

    \b
    Examples:
      meow kill mytask
      meow kill -r "^group/.*" --filter "task.status == 'running'"
    """

    if name and "backend" in name:
        console.print(
            "You cannot kill the backend using kill command. Use `meow exit` instead."
        )
        sys.exit(1)

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
                "[bold red]Are you sure you want to kill all tasks?[/bold red]", abort=True
            )

    response = BackendRequest.kill(name, force=force, use_regex=use_regex, filter=filter_)
    if response.error():
        console.print(response.detail)
        sys.exit(1)
    
    if yes:
        print_response(response)
