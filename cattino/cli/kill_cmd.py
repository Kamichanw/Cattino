import click
import sys
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import (
    fetch_from_msgbox,
    print_response,
    get_path_tree_str,
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
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def kill(all: bool, use_regex: bool, force: bool, filter_: str | None, name: str | None):
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
      meow kill -r "^group/.*" --filter "task.status == TaskStatus.Running"
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

    response = BackendRequest.kill(name, force=force, use_regex=use_regex, filter=filter_)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks killed successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success) } killed successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure) } failed to kill." if failure else None
        ),
    )
    if response.error():
        sys.exit(1)
