import builtins
import itertools
import os
import shlex
import sys
import click
import runpy
import re

from typing import Tuple

from cattino.cli.main import main
from cattino.cli.utils import (
    fetch_from_msgbox,
    print_response,
    RequiresMemo,
)
from cattino.cli.console import console
from cattino.constants import TASK_GLOBALS_KEY
from cattino.comms import BackendRequest, start_backend, where
from cattino.tasks.proc_task import ProcTask
from cattino.tasks.interface import DeviceRequiredTask, TaskGroup
from cattino.utils import (
    Magics,
    split_params,
)


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
    type=RequiresMemo(),
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
@click.argument("args", nargs=-1)
@fetch_from_msgbox
def create(
    input: str,
    task_name: str | None,
    priority: int | None,
    requires_memory_per_device: int | None,
    min_devices: int | None,
    multirun: bool,
    args: Tuple[str],
):
    """
    Create a new task from a Python script or a shell-like command string.

    The `input` can be a path to a Python script that exports task objects via
    `cattino.export`, or a quoted command string. You may pass task attributes
    and override defaults via options like `--task-name`, `--priority`, and
    device-related options. Use `--multirun` to expand list arguments into many
    independent tasks.

    \b
    Examples:
      meow create "python train.py --config conf.yaml"
      meow create script.py -n experiment1
    """
    if not BackendRequest.test().ok():
        start_backend()
    run_dir = where()
    if run_dir is None:
        console.print("Faild to start backend.")
        sys.exit(1)
    # add cwd to path to load modules in user-provided python script
    sys.path.insert(0, os.getcwd())
    extra_paths = sys.path

    # expand list arguments
    if multirun and args:

        def parse_list(value):
            return (
                split_params(value[1:-1])
                if value.startswith("[") and value.endswith("]")
                else [value]
            )

        folded_args = []
        for arg in args:
            if "=" in arg:
                key, value = arg.split("=", 1)
                folded_args.append(
                    [f"{key}={shlex.quote(v)}" for v in parse_list(value)]
                )
            else:
                folded_args.append(parse_list(arg))

        extra_args = builtins.list(itertools.product(*folded_args))
    else:
        extra_args = [args]

    # collect all keys in kwargs
    arg_keys = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("-"):
            if i + 1 < len(args) and not args[i + 1].startswith("-"):
                # it is not a flag, skip the its value
                arg_keys.append(arg)
                i += 1
        elif "=" in arg:
            key = arg.split("=", 1)[0]
            arg_keys.append(key)
        i += 1
    # replace {key} in task name
    fullnames = []
    for ex_arg in extra_args:
        if task_name is not None:
            params = {}
            for i, arg in enumerate(ex_arg):
                if "=" in arg:
                    key, value = arg.split("=", 1)
                    if key in arg_keys:
                        params[key] = value
                else:
                    if arg in arg_keys:
                        params[arg] = ex_arg[i + 1]
            formatted_name = re.sub(
                r"\{([^}]+)\}",
                lambda m: str(params.get(m.group(1), m.group(0))),
                task_name,
            )
            fullnames.append(formatted_name.rstrip("/"))
        else:
            fullnames.append(None)

    def override_attrs(task, name):
        if name is not None:
            task.name = name
        if priority is not None:
            task.priority = priority

        if isinstance(task, DeviceRequiredTask):
            if requires_memory_per_device is not None:
                task.requires_memory_per_device = requires_memory_per_device  # type: ignore
            if min_devices is not None:
                task.min_devices = min_devices  # type: ignore
        return task

    # case 1: input is a Python script
    if os.path.isfile(input) and input.endswith(".py"):
        original_argv = sys.argv
        tasks = []
        for fullname, ex_args in zip(fullnames, extra_args):
            sys.argv = [input] + builtins.list(ex_args)
            task_list = runpy.run_path(input, run_name="__main__").get(TASK_GLOBALS_KEY)
            if not task_list:
                console.print(
                    "The input file does not contain a valid task object with command\n"
                    f"python {' '.join(sys.argv)}\n"
                    "Please ensure you've exported a task object with `cattino.export`, "
                    "and there is no exception during execution."
                )
                sys.exit(1)

            tasks.append(
                [
                    override_attrs(
                        task, fullname.split("/")[-1] if fullname is not None else None
                    )
                    for task in task_list
                ]
            )
        sys.argv = original_argv

    # case 2: input is a command string
    else:
        try:
            cmds = [
                shlex.split(input) + builtins.list(ex_args) for ex_args in extra_args
            ]
        except ValueError as e:
            console.print(f"Invalid command string: {e}")
            sys.exit(1)
        cmd_strs = [
            Magics.resolve(
                " ".join(cmd),
                run_dir=run_dir,
                task_name=fullname.split("/")[-1] if fullname is not None else None,
                fullname=fullname,
            )
            for fullname, cmd in zip(fullnames, cmds)
        ]
        tasks = [
            [
                override_attrs(
                    ProcTask(cmd_str),
                    fullname.split("/")[-1] if fullname is not None else None,
                )
            ]
            for cmd_str, fullname in zip(cmd_strs, fullnames)
        ]

    if arg_keys:
        groups, expanded_tasks = {}, []
        for fullname, task_list in zip(fullnames, tasks):
            if "/" in fullname:
                # should form a task group
                group_name, rest_name = fullname.split("/", 1)
                # we need to remove name after the last '/', as it is task's name
                *rest_name, _ = rest_name.rsplit("/", 1)
                groups.setdefault(group_name, []).extend(
                    ("".join(rest_name), task) for task in task_list
                )
            else:
                expanded_tasks.extend(task_list)
        expanded_tasks.extend(
            TaskGroup.from_name_task_pairs(v, group_name=k) for k, v in groups.items()
        )
    else:
        expanded_tasks = builtins.list(itertools.chain.from_iterable(tasks))
    response = BackendRequest.create(expanded_tasks, extra_paths=extra_paths)
    print_response(response)
