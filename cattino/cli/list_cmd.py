import click
import sys
import time

from copy import deepcopy
from rich.table import Table
from rich.live import Live

from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import fetch_from_msgbox, MagicString
from cattino.cli.console import console


@main.command(name="list")
@click.option(
    "--filter",
    type=MagicString(),
    required=False,
    help="Filter tasks by a python expression that accept a argument 'task'.",
)
@click.argument("name", required=False, type=str)
@click.option(
    "--regex",
    "-r",
    "use_regex",
    is_flag=True,
    default=False,
    help="Interpret the provided name as a regular expression.",
)
@click.argument("attrs", nargs=-1, type=str, required=False)
@fetch_from_msgbox
def list_cmd(
    name: str | None,
    filter: str | None,
    use_regex: bool,
    attrs: tuple[str, ...],
):
    """
    List tasks.

    Show a live-updating table of tasks. Use `--filter` to narrow results with a
    Python expression that receives a `task` object. Provide attribute names as
    positional arguments to include extra columns (for example: `status`,
    `priority`). The display updates automatically.

    \b
    Examples:
      meow list status
      meow list --filter "task.status == waiting"
    """
    if "status" not in attrs:
        attrs = ("status",) + attrs
    if "fullname" not in attrs:
        attrs = ("fullname",) + attrs
    response = BackendRequest.list(name=name, filter=filter, attrs=attrs, use_regex=use_regex)
    if response.error():
        console.print(response.detail)
        sys.exit(1)

    if not (results := response.results):  # type: ignore
        console.print("No tasks found.")
        sys.exit(0)

    def make_table(results_list):
        t = Table("fullname", *attrs)
        for result in results_list:
            t.add_row(result["fullname"], *[str(result.get(attr) or "-") for attr in attrs])
        return t

    prev_snapshot = [(r["fullname"], tuple(r.get(a) for a in attrs)) for r in results]
    table = make_table(results)
    with Live(table, console=console, refresh_per_second=4) as live:
        while True:
            time.sleep(1.0)
            resp = BackendRequest.list(name=name, filter=filter, attrs=attrs, use_regex=use_regex)
            if resp.error():
                console.print(f"Error: {resp.detail}")
                break
            new_results = resp.results or []  # type: ignore
            new_snapshot = [
                (r["fullname"], tuple(r.get(a) for a in attrs)) for r in new_results
            ]
            if new_snapshot != prev_snapshot:
                prev_snapshot = deepcopy(new_snapshot)
                live.update(make_table(new_results))
