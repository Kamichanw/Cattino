import click
import sys
from cattino.comms import BackendRequest
from cattino.cli.main import main
from cattino.cli.utils import fetch_from_msgbox
from cattino.cli.console import console


@main.command()
@click.argument("name", type=str, required=False)
@fetch_from_msgbox
def test(name: str | None):
    """
    Check whether the backend or a specific task is running.

    Query the backend (or a task) for its running status. If the target is
    running the command will print a human-friendly message and the PID when
    available.

    \b
    Examples:
      meow test
      meow test mytask
    """

    response = BackendRequest.test(name)

    if response.error():
        console.print(response.detail)
        sys.exit(1)

    if name is None:
        name = "backend"
    if not response.ok():
        console.print(
            f"{name} does not exist, has not started yet, or has already ended."
        )
    else:
        pid = getattr(response, "pid", None)
        path = getattr(response, "path", None)
        msg = f"{name} is running"
        if pid is not None:
            msg += f" with PID {pid}"
        if path is not None:
            msg += f", located at {path}."
        console.print(msg)
