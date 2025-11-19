import threading
import time
import click
import sys
import re

from cattino.cli.console import console
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.utils import open_redirected_stream
from cattino.cli.utils import fetch_from_msgbox


@main.command()
@click.argument("fullname", type=str, required=False)
@click.option(
    "--stream",
    "-s",
    type=click.Choice(["stdout", "stderr"]),
    default="stdout",
    help="Stream to watch. Defaults to stdout.",
)
@fetch_from_msgbox
def watch(fullname: str | None, stream: str):
    """
    Redirect an output stream of the backend or a specific task to the terminal.

    If no task name is provided, the backend's output stream will be shown. Use
    `--stream` to choose `stdout` or `stderr`. The command follows the selected
    stream in real time and filters out progress-bar artifacts for readability.

    \b
    Examples:
      meow watch
      meow watch mytask --stream stderr
    """

    if (backend_response := BackendRequest.test()).error():
        console.print(backend_response.detail)
        sys.exit(1)

    if fullname is None:
        fullname = "backend"
    if (response := BackendRequest.test(fullname)).error() or getattr(
        response, "path", None
    ) is None:
        print(response.status_code)
        console.print(f"{fullname} does not exist or has not started yet.")
        sys.exit(1)

    task_cache_dir = getattr(response, "path")

    PROGRESS_BAR_PATTERN = re.compile(r"\d+%\|.*\| \d+/\d+")
    is_running = threading.Event()

    def running_test():
        """Monitor whether the process is running"""
        while not is_running.is_set():
            time.sleep(2)
            try:
                if not BackendRequest.test(fullname).ok():
                    is_running.set()
            except Exception:
                is_running.set()
                raise

    threading.Thread(target=running_test, daemon=True).start()

    with open_redirected_stream(task_cache_dir, stream, "r") as f:
        # filter progress bars, and only output the last states
        exist_lines = f.readlines()
        last_progress_bar = None
        for line in exist_lines:
            line = line.rstrip("\n")
            if PROGRESS_BAR_PATTERN.search(line):
                last_progress_bar = line
            else:
                if last_progress_bar:
                    console.print(last_progress_bar)
                    last_progress_bar = None
                console.print(line)

        if last_progress_bar:
            console.print(last_progress_bar, end="")

        # watch the stream in real-time
        # cache_nl ensures the progress bar is refreshed correctly. for tqdm, it outputs
        # the progress bar followed by a newline. ignoring this newline allows proper refreshing.
        cache_nl = False
        while not is_running.is_set():
            if (line := f.readline()) == "\n":
                cache_nl = True
            elif line:
                if PROGRESS_BAR_PATTERN.search(line):
                    click.echo("\r" + line, nl=False)
                else:
                    if cache_nl:
                        console.print()
                        cache_nl = False
                    console.print(line, end="")
            else:
                time.sleep(0.5)

        click.echo()
