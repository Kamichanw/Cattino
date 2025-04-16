from abc import ABC, abstractmethod
import itertools
import random
import string
import time
import enum

from typing import List, Literal, Optional, Union, TYPE_CHECKING
from collections import Counter

from catin.utils import is_valid_filename
from catin.core.device_allocator import DeviceAllocator


if TYPE_CHECKING:
    from catin.tasks.task_graph import TaskGraph


class TaskStatus(enum.Enum):
    Suspended = enum.auto()  # The task will be delayed until it is woken up.
    Waiting = enum.auto()
    Running = enum.auto()
    Done = enum.auto()  # The task has finished successfully
    Failed = enum.auto()  # The task has finished with an error


class AbstractTask:
    def __init__(self, name: str):
        self.name = name
        self._group: Optional["TaskGroup"] = None
        if self.name == "backend":
            raise ValueError("Task name cannot be 'backend'. It is reserved for catin.")
        if not is_valid_filename(self.name):
            raise ValueError(
                f"'{self.name}' is not a valid task name. It should be a valid dirname."
            )

    @property
    def group(self) -> Optional["TaskGroup"]:
        """Get the group that the task belongs to. If the task belongs to any group, it returns None."""
        return self._group

    @property
    def fullname(self) -> str:
        """
        Obtain the task's fullname, which is composed of the full group name and task name, separated by a `/`.
        For example, if task `t` is within group `g`, and group `g` is within group `G`, then the `fullname` of `t` would be "G.g.t".
        """
        if self.group:
            return f"{self.group.fullname}/{self.name}"
        return self.name

    def on_start(self) -> None:
        """
        Callback executed before the task starts.
        Subclasses may override this method to perform any additional actions before the task begins.
        Make sure to call the parent class method to ensure proper pre actions.
        """
        pass

    def on_end(self) -> None:
        """
        Callback executed after the task ends normally or is terminated.
        Subclasses may override this method to perform cleanup or other actions after task completion.
        Make sure to call the parent class method to ensure proper cleanup.
        """
        pass


class Task(AbstractTask, ABC):
    def __init__(self, task_name: Optional[str] = None, priority: int = 0):
        """
        Initialize an abstract backend task.

        This task serves as a blueprint for tasks executed in the backend.
        If no task name is provided, a random 5-character alphanumeric string is generated.

        Args:
            task_name (Optional[str], optional): The name of the task. Defaults to None.
            priority (int): The task's priority level. Higher values indicate higher priority.
                Defaults to 0.
        """
        super().__init__(
            task_name
            if task_name is not None
            else "".join(random.choices(string.ascii_letters + string.digits, k=5))
        )

        self.create_time = time.time_ns()
        self.priority = priority

    @abstractmethod
    def start(self) -> None:
        """
        Start the task.

        This method begins the task's execution. Subclasses must implement the logic to start the task.
        """
        if self.is_ready:
            # do your own logic here
            ...

    @abstractmethod
    def wait(self, timeout: Optional[float] = None) -> None:
        """
        Wait for the task to complete.

        This method should block until the task finishes execution. If the task is not running,
        it should perform no action.
        """
        if self.status == TaskStatus.Running:
            # do your own logic here
            ...

    @abstractmethod
    def suspend(self) -> None:
        """
        Postpone a task. If the task is currently running, terminate it and set its status to pending.
        """
        if self.status in [TaskStatus.Done, TaskStatus.Failed]:
            return
        if self.status == TaskStatus.Running:
            self.terminate()
        elif self.status == TaskStatus.Waiting:
            ...
        # set status to Pending

    @abstractmethod
    def resume(self) -> None:
        """
        Resume a pending task. If the task is not pending, it does nothing.
        """
        if self.status == TaskStatus.Suspended:
            # do your own logic here, set status to Waiting
            ...

    @abstractmethod
    def terminate(self, force: bool = False) -> None:
        """
        Terminate the task immediately.

        This method should stop the task's execution and set its status to Done or Failed.
        If force is True, it will forcefully terminate the task.
        If the task is not running, it does nothing.
        Once terminated, the task should not be executed again.
        """
        if self.status == TaskStatus.Running:
            # do your own logic here, set status to Done or Failed
            ...

    @property
    @abstractmethod
    def status(self) -> TaskStatus:
        """
        Get the current status of the task.

        This property must return the appropriate TaskStatus enum value based on the task's state.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def is_ready(self) -> bool:
        """
        Check if the task is ready to run.

        Returns:
            bool: True if the task is ready for execution, otherwise False.
        """
        if self.status == TaskStatus.Waiting:
            # do your own logic here
            ...
        return False


class DeviceRequiredTask(Task):
    def __init__(
        self,
        task_name: Optional[str] = None,
        priority: int = 0,
        visible_devices: Optional[List[int]] = None,
        requires_memory_per_device: int = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Base class for tasks that require device resource allocation.

        Args:
            requires_memory_per_device (int): Memory required per device (in MiB).
            min_devices (int): Minimum number of devices required.
            visible_devices (List[int]): List of visible device indices.
            task_name (Optional[str], optional): Name of the task. Defaults to None.
            priority (int, optional): Task priority. Defaults to 0.
        """
        super().__init__(task_name=task_name, priority=priority)
        self._allocator = DeviceAllocator()
        # this property will be set by the allocator
        self._assigned_device_indices: Optional[List[int]] = None

        self.requires_memory_per_device = requires_memory_per_device
        self.min_devices = min_devices
        self.visible_devices = (
            visible_devices or DeviceAllocator.get_all_device_indices()
        )

    @property
    def assigned_device_indices(self) -> Optional[List[int]]:
        """
        Get the assigned device indices.
        """
        return self._assigned_device_indices

    def acquire_devices(self) -> bool:
        """
        Acquire the required devices. If successful, the assigned device indices will be set,
        and return True. Otherwise, return False.
        """
        self._allocator.allocate(self)
        return self.assigned_device_indices is not None

    def release_devices(self) -> None:
        """
        Release the acquired devices.
        """
        self._allocator.release(self)

    def on_end(self):
        super().on_end()
        self.release_devices()

    @property
    def visible_device_environ(self) -> dict:
        """
        Get the environment variables for the visible devices.
        """
        if self.assigned_device_indices is None:
            return {}
        else:
            return self._allocator.get_device_control_env_var(
                self.assigned_device_indices
            )


class TaskGroup(AbstractTask):
    def __init__(
        self,
        tasks: List[AbstractTask],
        execute_strategy: Union[
            Literal["sequential", "parallel"], "TaskGraph"
        ] = "parallel",
        group_name: Optional[str] = None,
    ):
        """
        Initialize a task group.

        Args:
            tasks (List[AbstractTask], optional): The list of tasks and subgroups.
            execute_strategy (Union[str, TaskGraph], optional): The execution strategy for the group,
                either 'sequential', 'parallel', or a TaskGraph object.
            group_name (str, optional): The name of the task group. If not provided,
                it will be set to 'group_of_{name of a task in the group}'.

        """
        if len(tasks) == 0:
            raise ValueError("The task group must contain at least one task.")

        super().__init__(
            group_name if group_name is not None else f"group_of_{tasks[0].name}"
        )
        self.subtasks = tasks
        self._group: Optional["TaskGroup"] = None

        for t in tasks:
            t._group = self

        all_task_name = []
        for task in self.subtasks:
            if issubclass(type(task), TaskGroup):
                all_task_name.extend(t.fullname for t in task.all_tasks)
            else:
                all_task_name.append(task.fullname)

        duplicate_task_names = [
            item for item, count in Counter(all_task_name).items() if count > 1
        ]
        if duplicate_task_names:
            raise ValueError(
                f"Task names must be unique, but got duplicate task names: {duplicate_task_names}"
            )

        if isinstance(execute_strategy, str):
            from catin.tasks.task_graph import TaskGraph

            task_graph = TaskGraph()
            task_graph.add_tasks_from(tasks)
            if execute_strategy == "sequential":
                task_graph.add_edges_from(
                    [(tasks[i], tasks[i + 1]) for i in range(len(tasks) - 1)]
                )
        else:
            if execute_strategy.has_cycle():
                raise ValueError(
                    "The execute_strategy has a cycle, which will lead to deadlock."
                )
            strategy_tasks = set(execute_strategy.tasks)
            all_tasks = set(
                itertools.chain(
                    *[[t] if isinstance(t, Task) else t.all_tasks for t in tasks]
                )
            )
            if not strategy_tasks.issubset(all_tasks):
                raise ValueError(
                    f"{', '.join([t.name for t in strategy_tasks - all_tasks])} from execute_strategy "
                    "are not in the task group."
                )

            # add remaining tasks that are not in execute_strategy
            execute_strategy.add_tasks_from(all_tasks - strategy_tasks)
            task_graph = execute_strategy

        self.graph = task_graph

    @property
    def all_tasks(self) -> List[Task]:
        """
        Get all tasks in the task group.
        """
        return self.graph.tasks

    def __iter__(self):
        return iter(self.subtasks)

    def __len__(self):
        return len(self.subtasks)

    def get_task_by_name(self, name: str) -> Optional[Union[Task, "TaskGroup"]]:
        """
        Get a task or task group by its name.
        """
        for task in self.subtasks:
            if task.name == name:
                return task
        return self.graph.get_task_by_name(name)
