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
    help="Resume all tasks or match names by regex expressions.",
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
def resume(all: bool, filter_: str | None, name: str | None):
    """
    Resume cancelled or failed tasks.

    Resume tasks that are in `Cancelled` or `Failed` states. Targets can be
    specified by `name`, matched with `-A/--all` (regex), or selected with
    `--filter` (Python expression accepting `task`).

    \b
    Examples:
      meow resume mytask
      meow resume --filter "task.priority >= 10"
    """

    if not name and not all and not filter_:
        console.print("No task name provided.")
        sys.exit(1)

    response = BackendRequest.resume(name, use_regex=all, filter=filter_)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks resumed successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success) } resumed successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure) } failed to resume." if failure else None
        ),
    )
