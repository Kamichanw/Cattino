import click
import sys
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import fetch_from_msgbox, print_response, get_path_tree_str
from cattino.cli.console import console


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Cancel all tasks or match names by regex expressions.",
)
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def cancel(all: bool, name: str | None):
    """
    Cancel running or waiting tasks.

    Cancel tasks that are currently running or queued. Specify a task `name` or
    use `-A/--all` to select by name (regex). The command returns which tasks
    were cancelled and which failed.

    Examples:
      meow cancel mytask
      meow cancel -A "^group/.*"
    """

    if not name and not all:
        console.print("No task name provided.")
        sys.exit(1)

    response = BackendRequest.cancel(name, use_regex=all)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks cancelled successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success) } cancelled successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure) } failed to cancell." if failure else None
        ),
    )
