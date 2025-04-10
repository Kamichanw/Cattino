import sys
from typing import Union

from catin.tasks.interface import TaskGroup, AbstractTask
from catin.constants import TASK_GLOBALS_KEY
from catin.settings import settings


def export(task: Union[AbstractTask, TaskGroup]) -> None:
    """
    Register a task or task group to the global task list for catin engine scheduling.
    The registered task will be appended to the `catin.constants.TASK_GLOBALS_KEY` list in the caller
    module's global namespace.
    """

    caller_globals = sys._getframe(1).f_globals
    if TASK_GLOBALS_KEY not in caller_globals:
        caller_globals[TASK_GLOBALS_KEY] = []

    if issubclass(type(task), AbstractTask) or issubclass(type(task), TaskGroup):
        caller_globals[TASK_GLOBALS_KEY].append(task)
    else:
        raise TypeError(
            f"Task object must be a subclass of AbstractTask or TaskGroup, not {type(task)}"
        )


__all__ = ["export", "settings"]
