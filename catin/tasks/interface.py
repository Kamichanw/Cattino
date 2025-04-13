from abc import ABC, abstractmethod
from collections import Counter
import random
import string
import time
import enum

from typing import List, Optional, Union
from catin.utils import is_valid_filename
from catin.core.device_allocator import DeviceAllocator
from catin.tasks.task_graph import TaskGraph


class TaskStatus(enum.Enum):
    Suspended = enum.auto()  # The task will be delayed until it is woken up.
    Waiting = enum.auto()
    Running = enum.auto()
    Done = enum.auto()  # The task has finished successfully
    Failed = enum.auto()  # The task has finished with an error


class AbstractTask(ABC):
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
        self.name = task_name or "".join(
            random.choices(string.ascii_letters + string.digits, k=5)
        )
        if self.name == "backend":
            raise ValueError("Task name cannot be 'backend'. It is reserved for catin.")
        if not is_valid_filename(self.name):
            raise ValueError(f"'{self.name}' is not a valid task name. It should be a valid dirname.")
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

    def on_task_start(self) -> None:
        """
        Callback executed immediately after the task starts.
        Subclasses may override this method to perform any additional actions when the task begins.
        Make sure to call the parent class method to ensure proper cleanup.
        """
        pass

    def on_task_end(self) -> None:
        """
        Callback executed when the task ends normally or is terminated.
        Subclasses may override this method to perform cleanup or other actions after task completion.
        Make sure to call the parent class method to ensure proper cleanup.
        """
        pass

    def __copy__(self):
        new_task = type(self)(priority=self.priority)
        return new_task


class DeviceRequiredTask(AbstractTask):
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
            visible_devices or self._allocator.get_all_device_indices()
        )

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

    def on_task_end(self):
        super().on_task_end()
        self.release_devices()

    @property
    def assigned_device_indices(self) -> Optional[List[int]]:
        """
        Get the assigned device indices.
        """
        return self._assigned_device_indices

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


class TaskGroup:
    def __init__(
        self,
        tasks: Union[List[AbstractTask], TaskGraph],
        execute_strategy: str = "sequential",
        group_name: Optional[str] = None,
    ):
        """
        TaskGroup class to represent a group of tasks to be executed in the backend.
        These tasks will be executed based on the execution strategy.
        - 'sequential' will execute tasks one by one.
        - 'parallel' will execute all tasks concurrently.
        - 'dag' will execute tasks based on a given directed acyclic graph.

        Args:
            tasks (List[Task] or TaskGraph): The tasks to be executed
            execute_strategy (str): The execution strategy. Defaults to "sequential".
            group_name (str, optional): The name of the task group.
                If not provided, it will be "group contains {name of the first task}".
        """

        if execute_strategy not in ["sequential", "parallel", "dag"]:
            raise ValueError(
                f"Invalid execution strategy: {execute_strategy}. Choose from 'sequential' or 'parallel'."
            )

        if (
            isinstance(tasks, TaskGraph)
            and execute_strategy != "dag"
            or not isinstance(tasks, TaskGraph)
            and execute_strategy == "dag"
        ):
            raise ValueError(
                "The type of tasks and the execution strategy must match. If tasks is a DiGraph, the execution"
                f"strategy must be 'dag', but {execute_strategy=} and {type(tasks)=}."
            )

        if len(tasks) == 0:
            raise ValueError("The task group must contain at least one task.")

        if len(set(t.name for t in tasks)) != len(tasks):
            duplicate_task_names = [
                item
                for item, count in Counter(t.name for t in tasks).items()
                if count > 1
            ]
            raise ValueError(
                f"Task names must be unique, but got duplicate task names: {duplicate_task_names}"
            )

        self.execute_strategy = execute_strategy
        self.name = (
            group_name
            or f"group contains {tasks[0].name if isinstance(tasks, list) else tasks.tasks[0].name}"
        )
        if not isinstance(tasks, TaskGraph):
            tasks_graph = TaskGraph()
            tasks_graph.add_tasks_from(tasks)
            if execute_strategy == "sequential":
                tasks_graph.add_edges_from(
                    [(tasks[i], tasks[i + 1]) for i in range(len(tasks) - 1)]
                )
        else:
            tasks_graph = tasks

        self.graph: TaskGraph = tasks_graph

    def on_task_group_start(self) -> None:
        """
        A callback function called when the task group starts.
        """

    def on_task_group_end(self) -> None:
        """
        A callback function called when the task group ends.
        It will be called only if all tasks end normally.
        """

    @property
    def tasks(self):
        return self.graph.tasks

    def __iter__(self):
        return iter(self.graph)

    def __len__(self):
        return len(self.graph)
