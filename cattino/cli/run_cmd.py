import click
import sys

from cattino.comms import BackendRequest, start_backend
from cattino.cli.main import main
from cattino.cli.utils import fetch_from_msgbox
from cattino.cli.console import console


@main.command()
@click.option(
    "--host",
    type=str,
    required=False,
    help="Host address for the backend.",
)
@click.option(
    "--port",
    type=int,
    required=False,
    help="Port number for the backend.",
)
@fetch_from_msgbox
def run(host: str | None, port: int | None):
    """
    Start the backend in the foreground.

    Launch a backend process and attach to its stdout/stderr. If a backend is
    already running the command will print a message and exit. Use `--host` and
    `--port` to override the default binding.

    \b
    Examples:
      meow run
      meow run --port 8081
    """

    if not BackendRequest.test().ok():
        start_backend(blocking=True, host=host, port=port)
    else:
        console.print("Backend is already running. Use `meow watch` to see the output.")
