import click
import sys
from cattino.comms import BackendRequest
from cattino.cli.main import main
from cattino.cli.utils import fetch_from_msgbox
from cattino.cli.console import console


@main.command()
@fetch_from_msgbox
def test():
    """
    Check whether the backend is running.

    Query the backend for its running status. It will print a human-friendly message and the PID when
    available.

    \b
    Examples:
      meow test
    """

    response = BackendRequest.test()

    if response.error():
        console.print(response.detail)
        sys.exit(1)

    pid = getattr(response, "pid", None)
    path = getattr(response, "path", None)
    msg = f"Backend is running"
    if pid is not None:
        msg += f" with PID {pid}"
    if path is not None:
        msg += f", located at {path}."
    console.print(msg)
