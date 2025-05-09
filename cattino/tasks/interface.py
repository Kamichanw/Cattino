from abc import ABC, abstractmethod
import itertools
import random
import string
import time
import enum

from typing import Dict, List, Literal, Optional, Sequence, Set, Union, TYPE_CHECKING
from collections import Counter

from cattino.core.path_tree import PathTree
from cattino.utils import is_valid_filename
from cattino.core.device_allocator import DeviceAllocator


if TYPE_CHECKING:
    from cattino.tasks.task_graph import TaskGraph


class TaskStatus(enum.Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    MultiStatus = enum.auto()  # only used for group
    Cancelled = (
        enum.auto()
    )  # The task has been cancelled and will not be scheduled for execution.
    Waiting = enum.auto()
    Running = enum.auto()
    Done = enum.auto()  # The task has finished successfully
    Failed = enum.auto()  # The task has finished with an error

    def __str__(self):
        return self.name

    def __eq__(self, value: object) -> bool:
        return (isinstance(value, TaskStatus) and self.name == value.name) or (
            isinstance(value, str) and self.name == value.capitalize()
        )


class AbstractTask(ABC):
    SETTABLE_ATTRS = ["name"]

    def __init__(self, name: str):
        self.name = name
        self._group: Optional["TaskGroup"] = None
        if self.name == "backend":
            raise ValueError(
                "Task name cannot be 'backend'. It is reserved for cattino."
            )
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

    @property
    @abstractmethod
    def status(self) -> TaskStatus: ...

    @abstractmethod
    def cancel(self) -> None: ...

    @abstractmethod
    def resume(self) -> None: ...

    @abstractmethod
    def terminate(self, force: bool = False) -> None: ...


class Task(AbstractTask):
    SETTABLE_ATTRS = AbstractTask.SETTABLE_ATTRS + ["priority"]

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
        Start the task. Cattino ensures that is_ready is True when this function is called.

        This method begins the task's execution. Subclasses must implement the logic to start the task.
        """

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
    def cancel(self) -> None:
        """
        Cancel a task that hasn't started or done yet. If the task is running, it should be terminated.
        Once the task is cancelled, it won't be scheduled for execution.
        """
        if self.status in [TaskStatus.Done, TaskStatus.Failed]:
            return
        if self.status == TaskStatus.Running:
            self.terminate()
        # set status to Cancelled
        ...

    @abstractmethod
    def resume(self) -> None:
        """
        Resume a pending or executed task to a ready-to-start state.
        If task is running or waiting, it should do nothing. The subclasses should
        guarantee that all inner variables are reset to the initial state.

        Note that only cattino can call this method when the task has been executed.
        CLI users should use the `resume` command to a cancelled task.
        """
        if self.status in [TaskStatus.Running, TaskStatus.Waiting]:
            return
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
    SETTABLE_ATTRS = Task.SETTABLE_ATTRS + [
        "requires_memory_per_device",
        "min_devices",
    ]

    def __init__(
        self,
        task_name: Optional[str] = None,
        priority: int = 0,
        requires_memory_per_device: int = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Base class for tasks that require device resource allocation.

        Args:
            requires_memory_per_device (int): Memory required per device (in MiB).
            min_devices (int): Minimum number of devices required.
            task_name (Optional[str], optional): Name of the task. Defaults to None.
            priority (int, optional): Task priority. Defaults to 0.
        """
        super().__init__(task_name=task_name, priority=priority)
        self._allocator = DeviceAllocator()
        # this property will be set by the allocator
        self._assigned_device_indices: Optional[List[int]] = None

        self._requires_memory_per_device = requires_memory_per_device
        self._min_devices = min_devices

    @property
    def requires_memory_per_device(self) -> int:
        """
        Get the memory required per device.
        """
        return self._requires_memory_per_device

    @requires_memory_per_device.setter
    def requires_memory_per_device(self, value: int) -> None:
        """
        Set the memory required per device.
        """
        if self.status == TaskStatus.Running:
            raise RuntimeError(
                "Cannot set requires_memory_per_device while the task is running."
            )
        self._requires_memory_per_device = value

    @property
    def min_devices(self) -> int:
        """
        Get the minimum number of devices required for the task.
        """
        return self._min_devices

    @min_devices.setter
    def min_devices(self, value: int) -> None:
        """
        Set the minimum number of devices required for the task.
        """
        if self.status == TaskStatus.Running:
            raise RuntimeError("Cannot set min_devices while the task is running.")
        self._min_devices = value

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
        tasks: Sequence[AbstractTask],
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
        self.subtasks: List[AbstractTask] = []
        self._group: Optional["TaskGroup"] = None

        for t in tasks:
            self.add_task(t)

        all_tasks: List[Task] = list(
            itertools.chain(
                *[[t] if not issubclass(type(t), TaskGroup) else t.all_tasks for t in tasks]  # type: ignore
            )
        )

        if duplicate_task_names := [
            item
            for item, count in Counter(t.fullname for t in all_tasks).items()
            if count > 1
        ]:
            raise ValueError(
                f"Task names must be unique, but got duplicate task names: {', '.join(duplicate_task_names)}"
            )

        if isinstance(execute_strategy, str):
            from cattino.tasks.task_graph import TaskGraph

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
            all_tasks_set = set(all_tasks)
            if not strategy_tasks.issubset(all_tasks_set):
                raise ValueError(
                    f"{', '.join([t.fullname for t in strategy_tasks - all_tasks_set])} from execute_strategy "
                    "are not in the task group."
                )

            # add remaining tasks that are not in execute_strategy
            execute_strategy.add_tasks_from(list(all_tasks_set - strategy_tasks))
            task_graph = execute_strategy

        self.execute_graph = task_graph

    @property
    def all_tasks(self) -> List[Task]:
        """
        Get all tasks in the task group.
        """
        return self.execute_graph.tasks

    def cancel(self):
        """
        Cancel all tasks and groups in the group.
        """
        for subtask in self.subtasks:
            subtask.cancel()

    def resume(self):
        """
        Resume all tasks and groups in the group.
        """
        for subtask in self.subtasks:
            subtask.resume()

    def terminate(self, force: bool = False):
        """
        Terminate all tasks and groups in the group.
        """
        for subtask in self.subtasks:
            subtask.terminate(force=force)

    def add_task(self, task: AbstractTask):
        """
        Add a task or a task group to the current task group.
        """
        if task in self:
            raise ValueError(
                f"Task '{task.fullname}' is already in the group '{self.fullname}'."
            )

        if task.group is not None:
            raise ValueError(
                f"Task '{task.fullname}' is already in group '{task.group.fullname}'."
            )

        self.subtasks.append(task)
        task._group = self

    @property
    def status(self):
        """
        Get the status of the task group. If all subtasks have the same status, return that status.
        Otherwise, return TaskStatus.MultiStatus.
        """
        return (
            self.subtasks[0].status
            if all(self.subtasks[0].status == t.status for t in self.subtasks)
            else TaskStatus.MultiStatus
        )

    @classmethod
    def from_name_task_pairs(
        cls,
        name_task_pairs: Sequence[tuple[str, AbstractTask]],
        group_types: Optional[Dict[str, type["TaskGroup"]]] = None,
        execute_strategy: Union[
            Literal["sequential", "parallel"], "TaskGraph"
        ] = "parallel",
        group_name: Optional[str] = None,
    ) -> "TaskGroup":
        """
        Create a TaskGroup from name-task pairs.

        Args:
            name_task_pairs (Sequence[tuple[str, AbstractTask]]): A sequence of tuples containing task group fullnames and tasks.
                For root tasks, group name should be an empty string. For example, the following pairs:
                ```
                [
                    ("", task1),
                    ("group1", task2),
                    ("group1/group2", task3),
                ]
                ```
                will create a group with the following structure:
                ```
                group_name
                ├── task1
                └── group1
                    └── group2
                        └── task3
                ```
            group_types (Optional[Dict[str, type[TaskGroup]]], *optional*): A dictionary mapping group names to their respective TaskGroup types.
                For those unspecified, the current TaskGroup type will be used.
                Defaults to None.
            execute_strategy (Union[str, TaskGraph], *optional*): The execution strategy for the group,
                either 'sequential', 'parallel', or a TaskGraph object. Defaults to "parallel".
            group_name (Optional[str], *optional*): The name of the task group. If not provided,
                it will be set to 'group_of_{name of a task in the group}'.
                Defaults to None.
        Returns:
            TaskGroup: A TaskGroup based on the provided name-task pairs.

        """
        if group_types is None:
            group_types = {}
        name_task_pairs = [(name.rstrip("/"), task) for name, task in name_task_pairs]
        tree = PathTree[AbstractTask]()
        for name, task in name_task_pairs:
            if name:
                tree.set_node(f"{name}{tree.sep}{task.name}", task)

        def build_group(node):
            subtasks = []
            for name, child in node.children.items():
                if child.children:
                    subtasks.append(build_group(child))
                else:
                    if child.data is None or not issubclass(
                        type(child.data), AbstractTask
                    ):
                        raise ValueError(
                            f"Invalid task '{name}' in group '{node.name}'."
                        )
                    subtasks.append(child.data)
            group_cls = group_types.get(node.name, cls)
            return group_cls(
                subtasks,
                group_name=node.name,
            )

        return cls(
            [build_group(root) for root in tree.roots.values()]
            + [task for name, task in name_task_pairs if not name],
            execute_strategy=execute_strategy,
            group_name=group_name,
        )

    def __iter__(self):
        return iter(self.subtasks)

    def __len__(self):
        return len(self.subtasks)

    def __contains__(self, task: AbstractTask):
        while (group := task.group) is not None:
            if group == self:
                return True
            task = group
        return False
