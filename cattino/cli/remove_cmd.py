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
    help="Remove all tasks or match names by regex expressions.",
)
@click.option(
    "--filter",
    "filter_",
    type=MagicString(),
    required=False,
    help="Filter tasks by a python expression that accept a argument 'task'.",
)
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def remove(all: bool, filter_: str | None, name: str | None):
    """
    Remove tasks from the scheduler.

    Remove one or multiple tasks. Specify a `name`, use `-A/--all` to match names
    (regex) or `--filter` to select tasks via a Python expression that receives a
    `task` object. The backend (task named `backend`) cannot be removed.

    \b
    Examples:
      meow remove mytask
      meow remove --filter "task.name.startswith('temp')"
    """

    if name and "backend" in name:
        console.print("Backend cannot be removed.")
        sys.exit(1)
    if not name and not all and not filter_:
        console.print("No task name provided.")
        sys.exit(1)
    response = BackendRequest.remove(name, use_regex=all, filter=filter_)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks removed successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success)} removed successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure)} failed to remove." if failure else None
        ),
    )
