import sys

from catin.tasks.interface import AbstractTask, TaskGroup, Task
from catin.settings import settings
from catin.constants import TASK_GLOBALS_KEY
from catin.utils import Magics
from catin.comms import where


def export(task: AbstractTask) -> None:
    """
    Register a task or task group to the global task list for catin engine scheduling.
    The registered task will be appended to the `catin.constants.TASK_GLOBALS_KEY` list in the caller
    module's global namespace.
    """

    caller_globals = sys._getframe(1).f_globals
    if TASK_GLOBALS_KEY not in caller_globals:
        caller_globals[TASK_GLOBALS_KEY] = []

    if issubclass(type(task), Task) or issubclass(type(task), TaskGroup):
        caller_globals[TASK_GLOBALS_KEY].append(task)
    else:
        raise TypeError(
            f"Task object must be a subclass of Task or TaskGroup, not {type(task).__name__}"
        )


__all__ = ["export", "settings", "Magics", "where"]
