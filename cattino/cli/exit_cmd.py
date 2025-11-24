import socket
import sys
import psutil
import click
from cattino.cli.main import main
from cattino import settings
from cattino.comms import BackendRequest
from cattino.cli.utils import fetch_from_msgbox
from cattino.cli.console import console


@main.command(name="exit")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Forcefully kill the backend process if it is running."
)
@fetch_from_msgbox
def exit_cmd(force: bool = False):
    """
    Exit the backend process.

    Gracefully request the backend to exit. If `--force` is provided (when
    supported by the CLI invocation), attempt to find and kill the backend
    process directly.

    \b
    Examples:
      meow exit
      meow exit --force
    """

    if force:
        ip_address = socket.gethostbyname(settings.host)
        for proc in psutil.process_iter(attrs=["pid", "name"]):
            try:
                for conn in proc.net_connections(kind="inet"):
                    if conn.laddr.ip == ip_address and conn.laddr.port == settings.port:
                        proc.kill()
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    else:
        if (response := BackendRequest.exit()).error():
            console.print(response.detail)
            sys.exit(1)

    console.print("Backend exiting...")
