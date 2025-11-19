import os
import shutil
import click

from pathlib import Path
from datetime import datetime

from cattino.utils import get_cattino_home, get_cache_dir
from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino import settings
from cattino.cli.utils import fetch_from_msgbox, DateTime
from cattino.cli.console import console


@main.command()
@click.option(
    "--before",
    "-b",
    type=DateTime(fill_default="latest"),
    required=False,
    help="Specify the datetime before or on which cache files will be deleted.",
)
@click.option(
    "--after",
    "-a",
    type=DateTime(),
    required=False,
    help="Specify the datetime after or on which cache files will be deleted.",
)
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Clean all cache directories, including settings and logs.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Print cleaned cache directories or files.",
)
@fetch_from_msgbox
def clean(before: datetime | None, after: datetime | None, all: bool, verbose: bool):
    """
    Clean cache directories.

    Remove cached backend directories, logs, and settings. Use `--before` and
    `--after` to scope deletion by creation time. Use `-A/--all` to override
    time filters and delete everything under the cache directory.

    Examples:
      meow clean --before "2025-01-01"
      meow clean -A
    """

    cattino_home = get_cattino_home()
    response = BackendRequest.test()
    current_cache_dir = (
        get_cache_dir("backend", response.pid) if hasattr(response, "pid") else None  # type: ignore
    )

    def remove_cache(path: str, force: bool = False):
        if not os.path.exists(path):
            return False
        create_time = datetime.fromtimestamp(os.path.getctime(path)).replace(
            microsecond=0
        )
        if (
            force
            or (before and create_time <= before)
            or (after and create_time >= after)
        ):
            if current_cache_dir and (
                os.path.commonpath([current_cache_dir, path])
                in [current_cache_dir, path]
            ):
                console.print(f"{path} is currently in use, skipping deletion.")
                return False
            if verbose:
                console.print(f"Deleting: {path}")
            try:
                shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
                return True
            except OSError as e:
                console.print(f"Error deleting {path}: {e}")

        return False

    if all:
        if before or after:
            click.confirm(
                "--all/-A option will ignore datetime options and delete all cache files and settings. Continue?",
                abort=True,
            )
        if response.error():
            settings.clear()

    cache_list = [
        str(dirs.parent)
        for dirs in Path(cattino_home).rglob("backend")
        if dirs.is_dir()
    ]
    tree: dict = {}
    for path in cache_list:
        current = tree
        for part in Path(path.removeprefix(cattino_home)).parts:
            current = current.setdefault(part, {})

    def clean_dir_tree(prefix: str, d: dict):
        for k, v in d.copy().items():
            path = os.path.join(prefix, k)
            if v == {}:
                if remove_cache(path, force=all):
                    del d[k]
            else:
                if clean_dir_tree(path, v):
                    del d[k]

        return remove_cache(prefix, force=all) if prefix and not d else False

    tree[""] = tree.pop("/", {})
    clean_dir_tree(cattino_home, tree)

    console.print(f"Clean completed from {cattino_home}.")
