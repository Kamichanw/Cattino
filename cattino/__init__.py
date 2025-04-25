import sys

from cattino.tasks.interface import AbstractTask, TaskGroup, Task
from cattino.settings import settings
from cattino.constants import TASK_GLOBALS_KEY, CATTINO_RETRY_EXIT_CODE
from cattino.utils import Magics
from cattino.comms import where


def export(task: AbstractTask) -> None:
    """
    Register a task or task group to the global task list for cattino engine scheduling.
    The registered task will be appended to the `cattino.constants.TASK_GLOBALS_KEY` list in the caller
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

def exit_and_retry():
    """
    Exit the current process and retry the task.
    This function is useful for restarting the task that ended with a not fatal error,
    such as cuda out of memory.
    """
    sys.exit(CATTINO_RETRY_EXIT_CODE)

__all__ = ["export", "settings", "Magics", "where"]
