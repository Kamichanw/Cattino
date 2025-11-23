import importlib
import inspect
import itertools
import logging
import os
import re
import string
import sys
import psutil

from datetime import datetime
from loguru import logger
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
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
            if "__init__.py" not in os.listdir(path):
                return path
    os.makedirs(DEFAULT_CATTINO_HOME, exist_ok=True)
    return DEFAULT_CATTINO_HOME


@overload
def get_cache_dir(backend_pid: int | None = None) -> str:
    """
    Get the current cache directory. Since the cache directory is based on the create time of the backend process,
    this function needs the process ID. If the process ID is not provided, it will use the current process ID.
    """
    ...


@overload
def get_cache_dir(filename: str, backend_pid: int | None = None) -> str:
    """
    Get the current cache directory with given filename. Since the cache directory is based on the create time of the backend process,
    this function needs the process ID. If the process ID is not provided, it will use the current process ID.
    """
    ...


@overload
def get_cache_dir(task: "AbstractTask", backend_pid: int | None = None) -> str:
    """
    Get the current cache directory with given task. Since the cache directory is based on the create time of the backend process,
    this function needs the process ID. If the process ID is not provided, it will use the current process ID.
    """
    ...


def get_cache_dir(*args, **kwargs) -> str:
    task, filename, backend_pid = (
        kwargs.get("task"),
        kwargs.get("filename"),
        kwargs.get("backend_pid"),
    )
    if len(args) == 1:
        if isinstance(args[0], str):
            filename = args[0]
        elif args[0] is None or isinstance(args[0], int):
            backend_pid = args[0]
        else:
            task = args[0]
    elif len(args) == 2:
        if isinstance(args[0], str):
            filename = args[0]
        else:
            task = args[0]
        backend_pid = args[1]

    if task is not None and filename is not None:
        raise ValueError("Only one of task and filename should be provided.")

    if filename is not None:
        format_str = Magics.resolve(
            CACHE_DIR_FORMAT, task_name=filename, fullname=filename
        )
    elif task is not None:
        format_str = Magics.resolve(
            CACHE_DIR_FORMAT,
            task_name=task.name,
            fullname=task.fullname,
        )
    else:
        format_str = Magics.resolve(CACHE_DIR_FORMAT, task_name="", fullname="")

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


def has_param_type(func, types: tuple[type, ...], index: int | None = None) -> bool:
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
    filename: Union[str, Path], additional_reserved: Sequence[str] | None = None
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
    additional_reserved = tuple(additional_reserved) if additional_reserved else ()
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


def split_params(params_str: str) -> list[str]:
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
    def register_new_constant(cls, name: str, value: str):
        """Register a new magic variable."""
        from cattino.settings import settings

        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError(f"The name and value of the constant must be string.")
        settings.magic_constants = {**settings.magic_constants, name: value}


class InterceptHandler(logging.Handler):
    """
    InterceptHandler(logging.Handler)

    A logging.Handler that forwards standard library logging.LogRecord objects to Loguru's `logger`. 
    It adapts record information so the external logger receives the original message text, level, 
    and exception context, and so the reported caller location reflects the original logging 
    call site rather than internals of the logging module.
    """
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:  # type: ignore
            frame = frame.f_back  # type: ignore
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logger(colorize: bool = True):
    """
    Replace the fastapi logger with loguru logger.
    """
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: ^4}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    from cattino.settings import settings

    seen_once_messages = set()

    def log_filter(record) -> bool:
        if record["extra"].get("once"):
            message_key = (record["message"], record["file"].path, record["line"])
            if message_key in seen_once_messages:
                return False
            seen_once_messages.add(message_key)
        
        if not settings.debugging:
            return record["level"].no >= logger.level("INFO").no
        return True

    logger.add(
        sys.stdout,
        format=log_format,
        level="DEBUG",
        filter=log_filter,
        enqueue=True,
        backtrace=False,
        diagnose=True,
        colorize=colorize,
    )

    # forward all stdlib loggers to loguru, including 3rd-party libraries
    logger_name_list = [name for name in logging.root.manager.loggerDict]
    for logger_name in logger_name_list:
        _logger = logging.getLogger(logger_name)
        _logger.handlers = []
        if "." not in logger_name: # if it is a root logger
            _logger.addHandler(InterceptHandler())
