import importlib
import inspect
import itertools
import os
import re
import string
import sys
import psutil

from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Sequence,
    Union,
    get_args,
    get_origin,
    overload,
)

from cattino.constants import CATTINO_HOME, CACHE_DIR_FORMAT, DEFAULT_CATTINO_HOME

if TYPE_CHECKING:
    from cattino.tasks.interface import AbstractTask


def import_pynvml():
    """
    NOTE: This function is copied from vLLM's codebase.
    Historical comments:

    libnvml.so is the library behind nvidia-smi, and
    pynvml is a Python wrapper around it. We use it to get GPU
    status without initializing CUDA context in the current process.
    Historically, there are two packages that provide pynvml:
    - `nvidia-ml-py` (https://pypi.org/project/nvidia-ml-py/): The official
        wrapper. It is a dependency of vLLM, and is installed when users
        install vLLM. It provides a Python module named `pynvml`.
    - `pynvml` (https://pypi.org/project/pynvml/): An unofficial wrapper.
        Prior to version 12.0, it also provides a Python module `pynvml`,
        and therefore conflicts with the official one. What's worse,
        the module is a Python package, and has higher priority than
        the official one which is a standalone Python file.
        This causes errors when both of them are installed.
        Starting from version 12.0, it migrates to a new module
        named `pynvml_utils` to avoid the conflict.
    It is so confusing that many packages in the community use the
    unofficial one by mistake, and we have to handle this case.
    For example, `nvcr.io/nvidia/pytorch:24.12-py3` uses the unofficial
    one, and it will cause errors, see the issue
    https://github.com/vllm-project/vllm/issues/12847 for example.
    After all the troubles, we decide to copy the official `pynvml`
    module to our codebase, and use it directly.
    """
    import cattino.third_party.pynvml as pynvml

    return pynvml


def resolve_obj_by_qualname(fullname: str) -> Any:
    """
    Resolve an object by its fully qualified name.
    """
    module_name, obj_name = fullname.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, obj_name)


def get_cattino_home() -> str:
    """
    Get the path to the cache directory.
    """
    if CATTINO_HOME != DEFAULT_CATTINO_HOME:
        os.makedirs(CATTINO_HOME, exist_ok=True)
        return os.path.abspath(CATTINO_HOME)

    search_path = [
        os.path.join(os.getcwd(), "cattino-dev"),
        os.path.join(os.getcwd(), "cattino"),
    ]
    for path in search_path:
        if os.path.isdir(path):
            # if user installed cattino with editable mode, the source code will be
            # located in os.path.join(os.getcwd(), "cattino"). we can't save logs in
            # source dir, because `meow clean` will delete source code unexpectedly.
            if "setup.py" not in os.listdir(path):
                return path
    os.makedirs(DEFAULT_CATTINO_HOME, exist_ok=True)
    return DEFAULT_CATTINO_HOME


@overload
def get_cache_dir(filename: str, backend_pid: Optional[int] = None) -> str:
    """
    Get the current cache directory with given filename. Since the cache directory is based on the create time of the backend process,
    this function needs the process ID. If the process ID is not provided, it will use the current process ID.
    """
    ...


@overload
def get_cache_dir(task: "AbstractTask", backend_pid: Optional[int] = None) -> str:
    """
    Get the current cache directory with given task. Since the cache directory is based on the create time of the backend process,
    this function needs the process ID. If the process ID is not provided, it will use the current process ID.
    """
    ...


def get_cache_dir(filename_or_task, backend_pid=None) -> str:

    if isinstance(filename_or_task, str):
        format_str = Magics.resolve(
            CACHE_DIR_FORMAT, task_name=filename_or_task, fullname=filename_or_task
        )
    else:
        format_str = Magics.resolve(
            CACHE_DIR_FORMAT,
            task_name=filename_or_task.name,
            fullname=filename_or_task.fullname,
        )

    cache_dir = os.path.join(
        get_cattino_home(),
        datetime.fromtimestamp(psutil.Process(backend_pid).create_time()).strftime(
            format_str
        ),
    )
    return os.path.normpath(cache_dir)


def open_redirected_stream(cache_dir: str, stream: str, mode: str = "w") -> Any:
    """
    Open a stream for stdout or stderr with a specific mode in the cache directory.
    """
    assert stream in ["stdout", "stderr"]
    os.makedirs(cache_dir, exist_ok=True)
    return open(os.path.join(cache_dir, f"{stream}.log"), mode, buffering=1)


def has_param_type(func, types: tuple[type, ...], index: Optional[int] = None) -> bool:
    """
    Check whether a function has a parameter (at a given position or anywhere)
    whose type annotation matches any of the provided types.

    Args:
        func: The target function to inspect.
        types (tuple): A tuple of types to match against (e.g., (list, str)).
        index (int, *optional*): If specified, checks only the parameter at this position (0-based index).
               If None, checks all parameters.

    Returns:
        True if the specified parameter (or any parameter) is annotated with a type
        that matches any of the provided types (directly or via subclass).
        False otherwise.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    def matches(annotation):
        origin = get_origin(annotation)
        if origin is None:
            return any(
                issubclass(tp, annotation) for tp in types if isinstance(tp, type)
            )
        if origin is Union:
            return any(matches(arg) for arg in get_args(annotation))
        return any(issubclass(tp, origin) for tp in types if isinstance(tp, type))

    # if index is specified, only check that parameter
    if index is not None:
        if index >= len(params):
            return False
        ann = params[index].annotation
        return ann is not inspect.Parameter.empty and matches(ann)

    # check all parameters
    return any(
        matches(p.annotation)
        for p in params
        if p.annotation is not inspect.Parameter.empty
    )


def is_valid_filename(
    filename: Union[str, Path], additional_reserved: Optional[Sequence[str]] = None
):
    """
    Check if filename is a valid filename in current platform.
    """
    is_windows = os.name == "nt"
    unicode_filename = str(filename)

    # precheck
    if len(unicode_filename.strip()) == 0:
        return False

    # check length
    byte_ct = len(unicode_filename.encode(sys.getfilesystemencoding()))
    min_len, max_len = 1, 255
    if not min_len <= byte_ct < max_len:
        return False

    # check reserve keyworks
    additional_reserved = additional_reserved or ()
    _WINDOWS_RESERVED_FILE_NAMES = additional_reserved + (
        ("CON", "PRN", "AUX", "CLOCK$", "NUL")
        + tuple(
            f"{name:s}{num:d}"
            for name, num in itertools.product(("COM", "LPT"), range(0, 10))
        )
        + tuple(
            f"{name:s}{ssd:s}"
            for name, ssd in itertools.product(
                ("COM", "LPT"),
                ("\N{SUPERSCRIPT ONE}", "\N{SUPERSCRIPT TWO}", "\N{SUPERSCRIPT THREE}"),
            )
        )
    )
    _MACOS_RESERVED_FILE_NAMES = additional_reserved + (":",)

    if is_windows:
        if unicode_filename in _WINDOWS_RESERVED_FILE_NAMES:
            return False
    else:
        if unicode_filename in _MACOS_RESERVED_FILE_NAMES:
            return False
    unprintable_ascii_chars = [
        chr(c) for c in range(128) if chr(c) not in string.printable
    ]
    _INVALID_PATH_CHARS = "".join(unprintable_ascii_chars)
    _INVALID_FILENAME_CHARS = _INVALID_PATH_CHARS + "/"
    _INVALID_WIN_PATH_CHARS = _INVALID_PATH_CHARS + ':*?"<>|\t\n\r\x0b\x0c'
    _INVALID_WIN_FILENAME_CHARS = (
        _INVALID_FILENAME_CHARS + _INVALID_WIN_PATH_CHARS + "\\"
    )
    _RE_INVALID_FILENAME = re.compile(
        f"[{re.escape(_INVALID_FILENAME_CHARS):s}]", re.UNICODE
    )
    _RE_INVALID_WIN_FILENAME = re.compile(
        f"[{re.escape(_INVALID_WIN_FILENAME_CHARS):s}]", re.UNICODE
    )

    if _RE_INVALID_FILENAME.findall(unicode_filename):
        return False
    if is_windows and _RE_INVALID_WIN_FILENAME.findall(unicode_filename):
        return False

    return True


def split_params(params_str: str) -> List[str]:
    """
    Split a comma-separated string to a list of parameters.
    """
    params = []
    current_param = ""
    inside = {"'": 0, '"': 0, "(": 0, ")": 0, "{": 0, "}": 0, "[": 0, "]": 0}

    for char in params_str:
        if (
            char == ","
            and inside["'"] % 2 == 0
            and inside['"'] % 2 == 0
            and inside["("] == inside[")"]
            and inside["{"] == inside["}"]
            and inside["["] == inside["]"]
        ):
            params.append(current_param.strip())
            current_param = ""
        else:
            if char in inside:
                inside[char] += 1
            current_param += char
    if current_param:
        params.append(current_param.strip())
    return params


import re
from cattino.utils import split_params

class Magics:
    """Magic variables and resolvers for cattino."""

    @classmethod
    def resolve(cls, string: str, **kwargs) -> str:
        """Resolve magic variables and functions in a string."""
        from cattino.settings import settings

        MAGICS = re.compile(r"\${([^{}]*(?:\{[^{}]*\}[^{}]*)*)}")

        def _resolve(match):
            expr = match.group(1)
            try:
                if ":" in expr:
                    resolver_name, params_str = expr.split(":", 1)
                    params = [
                        cls.resolve(p, **kwargs) for p in split_params(params_str)
                    ]
                    return str(settings.resolvers[resolver_name](*params))
                else:
                    if expr in settings.magic_constants:
                        return settings.magic_constants[expr]
                    if kwargs.get(expr) is not None:
                        return kwargs[expr]
            except Exception:
                ...
            return match.group(0)

        new_string = MAGICS.sub(_resolve, string.strip())
        while string != new_string:
            string = new_string
            new_string = cls.resolve(new_string, **kwargs)
        return string

    @classmethod
    def register_new_resolver(cls, name: str, func):
        """Register a new resolver."""
        from cattino.settings import settings

        if not callable(func):
            raise ValueError(f"Resolver {name} is not callable.")
        settings.resolvers = {
            **settings.resolvers,
            name: func,
        }

    @classmethod
    def register_new_variable(cls, name: str):
        """Register a new magic variable."""
        from cattino.settings import settings

        if not isinstance(name, str):
            raise ValueError(f"Variable {name} is not a string.")
        settings.magic_vars = list(set(settings.magic_vars + [name]))

    @classmethod
    def register_new_constant(cls, name: str, value: str):
        """Register a new magic variable."""
        from cattino.settings import settings

        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError(f"The name and value of the constant must be string.")
        settings.magic_constants = {**settings.magic_constants, name: value}

