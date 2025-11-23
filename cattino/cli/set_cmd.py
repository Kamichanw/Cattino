import ast
import click
import sys

from typing import Any, get_origin, get_args

from cattino.cli.main import main
from cattino.comms import BackendRequest
from cattino.cli.utils import fetch_from_msgbox, MagicString
from cattino.cli.console import console
from cattino import settings


@main.command(help="Change settings or task attributes")
@click.option(
    "--regex",
    "-r",
    "use_regex",
    is_flag=True,
    default=False,
    help="Interpret the provided name as a regular expression.",
)
@click.option(
    "--filter",
    "filter_",
    type=MagicString(),
    required=False,
    help="Filter tasks by a python expression that accepts a `task` argument.",
)
@click.argument("args", nargs=-1, type=MagicString())
@fetch_from_msgbox
def set_cmd(args: tuple, use_regex: bool, filter_: str | None):
    """
    Change settings or task attributes.

    Update global settings or set attributes on a specific task. To modify a
    task attribute use the form `taskname attr` as the first and second argument. For
    global settings use the setting name. Values are parsed where possible.

    \b
    Examples:
        meow set log-level DEBUG
        meow set mytask retries 3
    """
    # Expect either 2 args (global setting) or 3 args (task attribute set)
    if len(args) == 2:
        # global setting: `meow set KEY VALUE`
        setting, value = args[0], args[1]
        key = setting.replace("-", "_")

        if key not in settings.default_settings:
            console.print(
                f"Invalid setting: {setting}. Use `meow set --help` to see available settings."
            )
            sys.exit(1)

        if value is None:
            console.print(
                "Value is required. Use `meow set --help` to see available settings."
            )
            sys.exit(1)

        # if there is a backend running, set for it
        if (response := BackendRequest.test()).ok() and getattr(
            response, "home"
        ) is not None:
            settings._home = response.home  # type: ignore

        def _coerce_value(expected_type: Any, raw: Any) -> Any:
            if not isinstance(raw, str):
                return raw

            lower = raw.strip().lower()
            # common boolean forms
            if expected_type is bool or lower in ("true", "false", "1", "0"):
                if lower in ("true", "1"):
                    return True
                if lower in ("false", "0"):
                    return False

            # try literal eval for lists/dicts/numbers/tuples
            try:
                val = ast.literal_eval(raw)
                return val
            except Exception:
                # fall back to original string
                return raw

        def _is_compatible(expected_type: Any, val: Any) -> bool:
            # Accept anything for Any
            if expected_type is Any:
                return True
            origin = get_origin(expected_type)
            # direct type check
            if origin is None:
                if isinstance(expected_type, type):
                    return isinstance(val, expected_type)
                return True
            # handle Union (including X | Y)
            if origin is None and hasattr(expected_type, "__args__"):
                args = get_args(expected_type)
                return any(_is_compatible(a, val) for a in args)
            if origin is list or origin is list:
                return isinstance(val, list)
            if origin is dict or origin is dict:
                return isinstance(val, dict)
            if origin is tuple:
                return isinstance(val, tuple)
            # fallback to permissive
            return True

        assert key is not None
        old_value = settings.all_settings[key]
        expected = settings.get_type(key)
        coerced = _coerce_value(expected, value)

        # check compatibility and warn/exit if mismatch
        if not _is_compatible(expected, coerced):
            console.print(
                f"Type mismatch: setting '{setting}' expects {expected!r} but got {type(coerced).__name__}"
            )
            sys.exit(1)

        setattr(settings, key, coerced)
        if settings.all_settings[key] != old_value:
            console.print(f"Setting {setting} updated to {coerced}.")
        else:
            console.print(f"Setting {setting} has already to be set to {coerced}.")
    elif len(args) == 3:
        # task attribute mode: `meow set TASK ATTR VALUE`
        name, attr, value = args[0], args[1], args[2]
        response = BackendRequest.set_task_attr(
            name, attr, value, use_regex=use_regex, filter=filter_
        )
        if getattr(response, "detail", None):
            console.print(response.detail)
        if response.error():
            sys.exit(1)
    else:
        # invalid usage
        console.print(
            "Invalid usage. Use `meow set KEY VALUE` or `meow set TASK ATTR VALUE`."
        )
        sys.exit(1)
