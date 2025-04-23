import gettext
import itertools
import os
import re
import runpy
import shlex
import shutil
import socket
import sys
import threading
import time
import click

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Sequence, Tuple

import psutil

from cattino import settings
from cattino.constants import TASK_GLOBALS_KEY
from cattino.comms import Request, Response, start_backend, where
from cattino.core.path_tree import PathTree
from cattino.tasks.proc_task import ProcTask
from cattino.tasks.interface import DeviceRequiredTask, TaskGroup
from cattino.utils import (
    Magics,
    get_cache_dir,
    get_cattino_home,
    open_redirected_stream,
    split_params,
)


def print_response(
    response: Response,
    success_msg_fn: Callable[[Optional[List[str]]], Optional[str]],
    failure_msg_fn: Callable[[Optional[List[str]]], Optional[str]],
    no_op_msg_fn: Optional[Callable[[Optional[List[str]]], Optional[str]]] = None,
):
    """
    Print the response from the backend. It takes a response object and
    success, failure, and optional no-op message producers. These producers optionally
    take a list of task names and returns a message to be printed.

    For success_msg_fn, it should additionally handle the case when the response.ok() is True.
    """
    success_msg = success_msg_fn(getattr(response, "success", None))
    failure_msg = failure_msg_fn(getattr(response, "failure", None))
    no_op_msg = no_op_msg_fn(getattr(response, "no_op", None)) if no_op_msg_fn else None

    def echo(msg: Optional[str]):
        if msg:
            click.echo(msg)

    if response.ok():
        echo(success_msg)
    else:
        if response.error():
            click.echo(response.detail)
            sys.exit(1)
        echo(success_msg)
        echo(no_op_msg)
        echo(failure_msg)
        echo(response.detail)


def get_path_tree_str(names: List[str]):
    tree = PathTree()
    for name in names:
        tree.set_node(name, None)
    return str(tree)


class MagicString(click.ParamType):
    name = "magic_string"

    def convert(self, value, param, ctx):
        if not isinstance(value, str):
            self.fail(f"{value} is not a valid string", param, ctx)

        return Magics.resolve(value)


class DateTime(click.DateTime):
    def __init__(
        self,
        formats: Optional[Sequence[str]] = None,
        fill_default: Literal["latest", "earliest"] = "earliest",
        **kwargs,
    ):
        super().__init__(
            formats=formats
            or [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d %H",
                "%Y-%m-%d",
                "%Y-%m",
                "%Y",
            ],
            **kwargs,
        )
        assert all(
            fmt
            for fmt in self.formats
            if fmt
            in [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d %H",
                "%Y-%m-%dT%H",
                "%Y-%m-%d",
                "%Y-%m",
                "%Y",
            ]
        )
        self.fill_default = fill_default

    def convert(
        self, value: str, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if self.fill_default == "earliest":
            return super().convert(value, param, ctx)
        if isinstance(value, datetime):
            return value

        for fmt in self.formats:
            try:
                date_obj = datetime.strptime(value, fmt)
                if fmt == "%Y":
                    return date_obj.replace(date_obj.year, 12, 31, 23, 59, 59)
                if fmt == "%Y-%m":
                    return date_obj.replace(
                        date_obj.year, date_obj.month, 31, 23, 59, 59
                    )
                if fmt == "%Y-%m-%d":
                    return date_obj.replace(
                        date_obj.year, date_obj.month, date_obj.day, 23, 59, 59
                    )
                if fmt in ["%Y-%m-%d %H", "%Y-%m-%dT%H"]:
                    return date_obj.replace(
                        date_obj.year,
                        date_obj.month,
                        date_obj.day,
                        date_obj.hour,
                        59,
                        59,
                    )
                if fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"]:
                    return date_obj.replace(
                        date_obj.year,
                        date_obj.month,
                        date_obj.day,
                        date_obj.hour,
                        date_obj.minute,
                        59,
                    )
                return date_obj

            except ValueError:
                continue

        formats_str = ", ".join(map(repr, self.formats))
        self.fail(
            gettext.ngettext(
                "{value!r} does not match the format {format}.",
                "{value!r} does not match the formats {formats}.",
                len(self.formats),
            ).format(value=value, format=formats_str, formats=formats_str),
            param,
            ctx,
        )


@click.group()
def main():
    """CLI tool for managing tasks."""
    pass


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
def run(host: Optional[str], port: Optional[int]):
    """
    Start the backend.
    """
    if not Request.test().ok():
        start_backend(blocking=True, host=host, port=port)
    else:
        click.echo("Backend is already running. Use `meow watch` to see the output.")


@main.command()
def meow():
    """
    Meow meow~
    """
    import pkg_resources  # type: ignore[import]

    click.echo(f"Cattino: {pkg_resources.get_distribution('cattino').version}")


@main.command()
@click.argument("name", type=str, required=False)
def test(name: Optional[str]):
    """
    Test whether the backend or a specific tasks is running. If the query target is running,
    the PID of the process will be printed.
    """
    response = Request.test(name)

    if response.error():
        click.echo(response.detail)
        sys.exit(1)

    if name is None:
        name = "backend"
    if not response.ok():
        click.echo(f"{name} does not exist, has not started yet, or has already ended.")
    else:
        if getattr(response, "pid", None):
            click.echo(f"{name} is running with PID {response.pid}.")  # type: ignore
        else:
            click.echo(f"{name} is running.")


@main.command()
@click.argument("input", type=str)
@click.option(
    "--task-name",
    "-n",
    type=str,
    required=False,
    help="Optional task name. Defaults to a random 5-character alphanumeric string.",
)
@click.option(
    "--priority",
    "-p",
    type=int,
    required=False,
    help="Priority of the task. Defaults to 0.",
)
@click.option(
    "--requires-memory-per-device",
    "-M",
    type=int,
    required=False,
    help="Memory required per device in MiB. Defaults to 0.",
)
@click.option(
    "--min-devices",
    "-c",
    type=int,
    required=False,
    help="Minimum number of devices required. Defaults to 1.",
)
@click.option(
    "--multirun",
    "-m",
    is_flag=True,
    default=False,
    help='Expand list arguments after "--" into multiple independent commands.',
)
@click.option(
    "--as-group",
    "-g",
    required=False,
    type=str,
    help="Pack tasks into a group with given name.",
)
@click.argument("args", nargs=-1)
def create(
    input: str,
    task_name: Optional[str],
    priority: Optional[int],
    requires_memory_per_device: Optional[int],
    min_devices: Optional[int],
    multirun: bool,
    as_group: Optional[str],
    args: Tuple[str],
):
    """
    Create a new task from a Python script or command string.
    To create a task from a Python script, use `cattino.export` to export an object
    inheriting from `cattino.tasks.Task` or `cattino.tasks.TaskGroup` in that Python script.
    """
    if not Request.test().ok():
        start_backend()
    run_dir = where()
    fullname = f"{as_group}/{task_name}" if as_group and task_name else None
    if run_dir is None:
        click.echo("Faild to start backend.")
        sys.exit(1)
    # add cwd to path to load modules in user-provided python script
    sys.path.insert(0, os.getcwd())
    extra_paths = sys.path

    if multirun and args:
        list_args = []
        parse_list = lambda value: (
            split_params(value[1:-1])
            if value.startswith("[") and value.endswith("]")
            else [value]
        )

        for arg in args:
            arg = Magics.resolve(
                arg, run_dir=run_dir, task_name=task_name, fullname=fullname
            )

            if "=" in arg:
                key, value = arg.split("=", 1)
                list_args.append([f"{key}={v}" for v in parse_list(value)])
            else:
                list_args.append(parse_list(arg))

        extra_args = list(itertools.product(*list_args))
    else:
        extra_args = [args]

    def override_attrs(task):
        if task_name is not None:
            task.name = task_name
        if priority is not None:
            task.priority = priority

        if issubclass(type(task), DeviceRequiredTask):
            if requires_memory_per_device is not None:
                task.requires_memory_per_device = requires_memory_per_device  # type: ignore
            if min_devices is not None:
                task.min_devices = min_devices  # type: ignore
        return task

    # case 1: input is a Python script
    if os.path.isfile(input) and input.endswith(".py"):
        original_argv = sys.argv
        tasks = []
        for ex_args in extra_args:
            sys.argv = [input] + list(ex_args)
            task_list = runpy.run_path(input, run_name="__main__").get(TASK_GLOBALS_KEY)
            if not task_list:
                click.echo(
                    "The input file does not contain a valid task object with command\n"
                    f"python {' '.join(sys.argv)}\n"
                    "Please ensure you've exported a task object with `cattino.export`, "
                    "and there is no exception during execution."
                )
                sys.exit(1)

            tasks.append([override_attrs(task) for task in task_list])
        sys.argv = original_argv

    # case 2: input is a command string
    else:
        try:
            cmds = [shlex.split(input) + list(ex_args) for ex_args in extra_args]
        except ValueError as e:
            click.echo(f"Invalid command string: {e}")
            sys.exit(1)
        cmd_strs = [
            Magics.resolve(
                " ".join(cmd), run_dir=run_dir, task_name=task_name, fullname=fullname
            )
            for cmd in cmds
        ]
        tasks = [[override_attrs(ProcTask(cmd_str))] for cmd_str in cmd_strs]

    expanded_tasks = [task for task_list in tasks for task in task_list]
    response = Request.create(
        (
            [
                TaskGroup(
                    expanded_tasks, execute_strategy="parallel", group_name=as_group
                )
            ]
            if as_group
            else expanded_tasks
        ),
        extra_paths=extra_paths,
    )
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks created successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success)} created successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure)} failed to create." if failure else None
        ),
    )


@main.command()
@click.argument("name", nargs=-1, type=str, required=False)
def monitor(name):
    """
    List all tasks.
    """
    click.echo(name)


@main.command()
@click.argument("fullname", type=str, required=False)
@click.option(
    "--stream",
    "-s",
    type=click.Choice(["stdout", "stderr"]),
    default="stdout",
    help="Stream to watch. Defaults to stdout.",
)
def watch(fullname: Optional[str], stream: str):
    """
    Redirect a output stream of backend or a specific task to terminal.
    If no task name is provided, the backend's output stream will be redirected.
    """
    backend_response = Request.test()
    if backend_response.error():
        click.echo(backend_response.detail)
        sys.exit(1)
    backend_pid = getattr(backend_response, "pid")

    if fullname is None:
        fullname = "backend"

    task_cache_dir = get_cache_dir(fullname, backend_pid)
    if not os.path.exists(os.path.join(task_cache_dir, f"{stream}.log")):
        click.echo(f"{fullname} does not exist or has not started yet.")
        sys.exit(1)

    PROGRESS_BAR_PATTERN = re.compile(r"\d+%\|.*\| \d+/\d+")
    is_running = threading.Event()

    def running_test():
        """Monitor whether the process is running"""
        while not is_running.is_set():
            time.sleep(2)
            try:
                if not Request.test(fullname).ok():
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
                    click.echo(last_progress_bar)
                    last_progress_bar = None
                click.echo(line)

        if last_progress_bar:
            click.echo(last_progress_bar, nl=False)

        # watch the stream in real-time
        # cache_nl ensures the progress bar is refreshed correctly. for tqdm, it outputs
        # the progress bar followed by a newline. ignoring this newline allows proper refreshing.
        cache_nl = False
        while not is_running.is_set():
            line = f.readline()
            if line == "\n":
                cache_nl = True
            elif line:
                if PROGRESS_BAR_PATTERN.search(line):
                    click.echo("\r" + line, nl=False)
                else:
                    if cache_nl:
                        click.echo()
                        cache_nl = False
                    click.echo(line, nl=False)
            else:
                time.sleep(0.5)

        click.echo()


@main.command
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Suspend all tasks or match names by regex expressions.",
)
@click.argument("name", type=str, required=False)
def suspend(all: bool, name: Optional[str]):
    """
    Suspend specific task or group by full name or regex expressions. If the task is running,
    it will be terminated forcefully. Note that the end hooks of the task will not be called.
    """
    if not name and not all:
        click.echo("No task name provided.")
        sys.exit(1)

    response = Request.suspend(name, use_regex=all)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks suspended successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success) } suspended successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure) } failed to suspend." if failure else None
        ),
        lambda no_op: (
            f"{get_path_tree_str(no_op) } are not in waiting status." if no_op else None
        ),
    )


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Resume all tasks or match names by regex expressions.",
)
@click.argument("name", type=str, required=False)
def resume(all: bool, name: Optional[str]):
    """
    Resume specific task or group by full name or regex expressions. If the task is not in suspended
    status, it will be ignored.
    """
    if not name and not all:
        click.echo("No task name provided.")
        sys.exit(1)

    response = Request.resume(name, use_regex=all)
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
        lambda no_op: (
            f"{get_path_tree_str(no_op) } are not in suspended status."
            if no_op
            else None
        ),
    )


@main.command()
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Kill all running tasks or match names by regex expressions.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force kill tasks.",
)
@click.argument("name", type=str, required=False)
def kill(all: bool, force: bool, name: Optional[str]):
    """
    Kill specific task or group by full name or regex expressions.
    If you want to terminate the backend, use `meow exit` instead.
    """
    if name and "backend" in name:
        click.echo(
            "You cannot kill the backend using kill command. Use `meow exit` instead."
        )
        sys.exit(1)

    if not name and not all:
        click.echo("No task name provided.")
        sys.exit(1)

    response = Request.kill(name, force=force, use_regex=all)
    print_response(
        response,
        lambda success: (
            f"{len(success) if success else 0} tasks killed successfully."
            if response.ok()
            else (
                f"{get_path_tree_str(success) } killed successfully."
                if success
                else None
            )
        ),
        lambda failure: (
            f"{get_path_tree_str(failure) } failed to kill." if failure else None
        ),
        lambda no_op: (
            f"{get_path_tree_str(no_op) } are not running." if no_op else None
        ),
    )


@main.command
@click.option(
    "--all",
    "-A",
    is_flag=True,
    default=False,
    help="Remove all tasks or match names by regex expressions.",
)
@click.argument("name", type=str, required=False)
def remove(all: bool, name: Optional[str]):
    """
    Remove specific task or group by full name or regex expressions.
    If a task is running, it will be terminated forcibly.
    In this case, if `cascade-cancel-on-failure` is set to `True`, all subsequent tasks
    will be cancelled as well. To avoid this, use `meow kill` to terminate the tasks first.
    """
    if name and "backend" in name:
        click.echo("Backend cannot be removed.")
        sys.exit(1)
    if not name and not all:
        click.echo("No task name provided.")
        sys.exit(1)

    response = Request.remove(name, use_regex=all)
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


@main.command()
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    type=bool,
    help=f"Forcefully kill processes occupying the host: {settings.host} and port: {settings.port}.",
)
def exit(force: bool):
    """
    Exit the backend. even if backend may not respond.
    If you want to call end hooks of running tasks properly, use `meow kill --all` instead.
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
        response = Request.exit()
        if response.error():
            click.echo(response.detail)
            sys.exit(1)

    click.echo("Backend exiting...")


def retrieve_setting_help():
    """
    Retrieve the documentation for all settings with the following format:
    - `setting_name` (`setting_type`): `setting_description`
    """
    keys = list(settings.default_settings.keys())
    types = [settings.get_type(key).__name__ for key in keys]
    descriptions = [settings.get_description(key) for key in keys]
    return "\n".join(
        f"- {key.replace('_', '-')} ({type}): {description}"
        for key, type, description in zip(keys, types, descriptions)
    )


set_docstring = f"""
Change the settings of cattino.

\b
Available settings:
\b
{retrieve_setting_help()}
"""


@main.command(help=set_docstring)
@click.option(
    "--reset",
    "-r",
    is_flag=True,
    default=False,
    help="Reset a specific setting or all settings to default values.",
)
@click.option(
    "--show",
    "-s",
    is_flag=True,
    default=False,
    help="Show all current settings.",
)
@click.argument("setting", type=str, required=False)
@click.argument("value", type=MagicString(), required=False)
def set(reset: bool, show: bool, setting: Optional[str], value: Optional[str]):
    if show:
        if reset or setting or value:
            click.echo(
                "--show/-s option cannot be used with other options or arguments"
            )
            sys.exit(1)
        click.echo(
            "\n".join(
                f"{k.replace('_', '-')}: {v}"
                for k, v in sorted(settings.all_settings.items())
            )
        )
        sys.exit(0)

    key = setting.replace("-", "_") if setting else None
    if reset and setting is None:
        settings.clear()
        click.echo("All settings reset to default values.")
        sys.exit(0)

    if key not in settings.default_settings:
        click.echo(
            f"Invalid setting: {setting}. Use `meow set --help` to see available settings."
        )
        sys.exit(1)

    if reset:
        if value:
            click.echo("--reset/-r option cannot be used with a value. ")
            sys.exit(1)
        value = settings.default_settings[key]
    else:
        if value is None:
            click.echo(
                "Value is required. Use `meow set --help` to see available settings."
            )
            sys.exit(1)

    try:
        assert key is not None
        old_value = settings.all_settings[key]
        setattr(settings, key, value)
        if settings.all_settings[key] != old_value:
            click.echo(f"Setting {setting} updated to {value}.")
    except Exception as e:
        click.echo(f"Error setting {setting}: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--before",
    "-b",
    type=DateTime(
        fill_default="latest",
    ),
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
def clean(
    before: Optional[datetime], after: Optional[datetime], all: bool, verbose: bool
):
    """
    Clean up the cache directory based on the specified date and time options.
    """
    cattino_home = get_cattino_home()
    response = Request.test()
    current_cache_dir = (
        get_cache_dir("backend", response.pid) if hasattr(response, "pid") else None  # type: ignore
    )

    def remove_cache(path: str, force: bool = False):
        if not os.path.exists(path):
            return False
        # NOTE: in some platforms, getctime may return the last modified time
        # instead of the creation time
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
                click.echo(f"{path} is currently in use, skipping deletion.")
                return False
            if verbose:
                click.echo(f"Deleting: {path}")
            try:
                shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
                return True
            except OSError as e:
                click.echo(f"Error deleting {path}: {e}")

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
    # build directory tree to remove empty parent dir
    tree: dict = {}
    for path in cache_list:
        current = tree
        for part in Path(path.removeprefix(cattino_home)).parts:
            current = current.setdefault(part, {})

    def clean_dir_tree(prefix: str, d: dict):
        for k, v in d.copy().items():
            path = os.path.join(prefix, k)
            if v == {}:
                # now, we have reached the leaf node
                if remove_cache(path, force=all):
                    del d[k]
            else:
                if clean_dir_tree(path, v):
                    del d[k]

        return remove_cache(prefix, force=all) if prefix and not d else False

    # change the key name of the root directory to ensure
    # os.path.join works correctly. Otherwise, os.path.join(path, "/")
    # will always return the root directory "/".
    tree[""] = tree.pop("/", {})
    clean_dir_tree(cattino_home, tree)

    click.echo(f"Clean completed from {cattino_home}.")


if __name__ == "__main__":
    main()
