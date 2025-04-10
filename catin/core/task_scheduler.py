import asyncio
import logging
import aiorwlock
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    List,
    Dict,
    Sequence,
    Union,
    Optional,
)

from catin.tasks.interface import AbstractTask, TaskGroup, TaskStatus
from catin.tasks.task_graph import TaskGraph
from catin.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class GroupInfo:
    total: set  # total = done + remaining + running
    done: set
    remaining: set
    on_group_start: Callable[[], None]
    on_group_end: Callable[[], None]

    total_lock: aiorwlock.RWLock = field(default_factory=aiorwlock.RWLock)
    done_lock: aiorwlock.RWLock = field(default_factory=aiorwlock.RWLock)
    remaining_lock: aiorwlock.RWLock = field(default_factory=aiorwlock.RWLock)


class TaskScheduler:
    """
    Manages the execution of tasks, ensuring they are executed in the correct order and handling task dependencies.
    """

    def __init__(self) -> None:
        # when a task is dispatched, it will be added to the tasks graph
        # and the task will be scheduled by self.step. A task can be started if:
        #       - it has no predecessors
        #       - it has the highest priority
        #       - it is created the earliest
        # only a task is finished, it will be removed from the pending tasks
        self._pending_tasks = TaskGraph()
        self._pending_tasks_lock = aiorwlock.RWLock()

        # if a task starts, it will be added to the executed tasks
        self._executed_tasks: List[AbstractTask] = []
        self._executed_tasks_lock = aiorwlock.RWLock()

        # a dict to record the group info of a task to call on_task_group_* properly
        self._group_info_of_task: Dict[AbstractTask, GroupInfo] = {}

        # group id to tasks, this is used to resume task order once the task is paused
        self._groups: Dict[int, List[AbstractTask]] = {}

    async def step(self) -> bool:
        """
        Try to execute a waiting task if it is ready. Returns True if any tasks
        were executed, otherwise returns False.
        This method will check for exceptions in the executed tasks and raise
        TaskExecutionError if any exceptions were found.
        """

        if all(task.status == TaskStatus.Suspended for task in self._pending_tasks):
            return False

        async def execute(task: AbstractTask):
            group_info = self._group_info_of_task[task]

            async with group_info.remaining_lock.writer_lock:
                group_info.remaining.remove(task)
                async with group_info.total_lock.reader_lock:
                    is_first = len(group_info.remaining) == len(group_info.total) - 1

            try:
                task.start()

                if is_first:
                    self._group_info_of_task[task].on_group_start()

                task.on_task_start()
                await asyncio.to_thread(task.wait)
            except Exception as e:
                logger.exception(e)

            if task.status in [TaskStatus.Done, TaskStatus.Failed]:
                try:
                    task.on_task_end()
                except Exception as e:
                    logger.exception(e)

                async def cascade_cancel(task):
                    successors = self._pending_tasks.get_successors(task)
                    for t in successors:
                        await cascade_cancel(t)
                        group_info = self._group_info_of_task[t]
                        async with group_info.remaining_lock.writer_lock:
                            if t in group_info.remaining:
                                group_info.remaining.remove(t)
                        async with group_info.total_lock.writer_lock:
                            if t in group_info.total:
                                group_info.total.remove(t)
                        self._pending_tasks.remove_task(t)

                if (
                    settings.cascade_cancel_on_failure
                    and task.status == TaskStatus.Failed
                ):
                    async with self._pending_tasks_lock.writer_lock:
                        await cascade_cancel(task)

                group_info = self._group_info_of_task[task]
                async with group_info.done_lock.writer_lock:
                    group_info.done.add(task)
                    n_done = len(group_info.done)
                async with group_info.total_lock.reader_lock:
                    n_total = len(group_info.total)

                if n_done == n_total:
                    self._group_info_of_task[task].on_group_end()

                async with self._pending_tasks_lock.writer_lock:
                    self._pending_tasks.remove_task(task)

        async with self._executed_tasks_lock.reader_lock:
            async with self._pending_tasks_lock.reader_lock:
                outter_nodes = [
                    task
                    for task, ind in self._pending_tasks.in_degree.items()
                    if ind == 0 and task not in self._executed_tasks
                ]

        outter_nodes = sorted(
            outter_nodes,
            key=lambda task: (-task.priority, task.create_time),
        )

        has_executed = False  # whether at least one task is executed

        for upcoming_task in outter_nodes:
            if upcoming_task.is_ready:
                async with self._executed_tasks_lock.writer_lock:
                    self._executed_tasks.append(upcoming_task)
                asyncio.create_task(execute(upcoming_task))
                has_executed = True

        return has_executed

    async def get_task(self, task_name: str) -> Optional[AbstractTask]:
        async with self._executed_tasks_lock.reader_lock:
            for task in self._executed_tasks:
                if task.name == task_name:
                    return task
        async with self._pending_tasks_lock.reader_lock:
            return self._pending_tasks.get_task_by_name(task_name)

    @property
    async def running_tasks(self) -> List[AbstractTask]:
        """
        Get all running tasks.
        """
        async with self._executed_tasks_lock.reader_lock:
            return [
                task
                for task in self._executed_tasks
                if task.status == TaskStatus.Running
            ]

    @property
    async def all_tasks(self) -> List[AbstractTask]:
        """
        Get all tasks, including executed and pending tasks.
        """
        async with self._executed_tasks_lock.reader_lock:
            async with self._pending_tasks_lock.reader_lock:
                return list(set(self._executed_tasks + self._pending_tasks.tasks))

    async def get_task_status(self, query_keys: List[str]) -> List[List[Optional[Any]]]:
        """
        Get the status of all tasks based on the query keys.
        """
        return [
            [getattr(task, key, None) for key in query_keys]
            for task in await self.all_tasks
        ]

    async def dispatch(
        self,
        task: Union[AbstractTask, TaskGroup],
    ) -> None:
        """
        Enqueue a task or a task group.
        """
        if isinstance(task, AbstractTask):
            task_group = TaskGroup([task])
        elif isinstance(task, TaskGroup):
            task_group = task
        else:
            raise TypeError(
                f"`task` must be either a AbstractTask or a AbstractTaskGroup, but got {type(task)}"
            )

        async with self._pending_tasks_lock.reader_lock:
            if self._pending_tasks.test_cycle(task_group.graph):
                raise ValueError(
                    f"After adding the task group {task_group.name}, the task graph has a cycle. "
                    "This will lead to deadlock. "
                )

        all_tasks_name = set(t.name for t in await self.all_tasks)
        duplicate_task_names = set(t.name for t in task_group.graph) & all_tasks_name
        if duplicate_task_names:
            if not settings.override_exist_tasks:
                raise ValueError(
                    f"Tasks with the same name can be executed only once, but got duplicate task names: {', '.join(duplicate_task_names)}. "
                    "Use `meow set override-exist-tasks True` to suppress this error."
                )
            await self.remove(
                [
                    await self.get_task(name) for name in duplicate_task_names  # type: ignore
                ]
            )

        group_info = GroupInfo(
            total=set(task_group.tasks),
            remaining=set(task_group.tasks),
            done=set(),
            on_group_start=task_group.on_task_group_start,
            on_group_end=task_group.on_task_group_end,
        )

        for t in task_group.graph:
            self._group_info_of_task[t] = group_info

        self._pending_tasks.merge(task_group.graph)

    async def resume(self, task: AbstractTask) -> None:
        if task.status == TaskStatus.Suspended:
            task.resume()

    async def suspend(self, task: AbstractTask) -> None:
        if task.status in [TaskStatus.Running, TaskStatus.Waiting]:
            task.suspend()
            async with self._executed_tasks_lock.writer_lock:
                if task in self._executed_tasks:
                    self._executed_tasks.remove(task)
            group_info = self._group_info_of_task[task]
            async with group_info.remaining_lock.writer_lock:
                group_info.remaining.add(task)

    async def remove(self, tasks: Sequence[AbstractTask]) -> None:
        """
        Remove a task from the pending tasks or executed tasks.
        If the task is running, terminate it forcefully.
        """
        # we must remove the task from the pending tasks first
        # to ensure the on_task_group_end is called properly
        for task in tasks:
            async with self._pending_tasks_lock.writer_lock:
                if task in self._pending_tasks.tasks:
                    if task.status in [TaskStatus.Waiting, TaskStatus.Suspended]:
                        group_info = self._group_info_of_task[task]
                        async with group_info.remaining_lock.writer_lock:
                            if task in group_info.remaining:
                                group_info.remaining.remove(task)
                        async with group_info.total_lock.writer_lock:
                            if task in group_info.total:
                                group_info.total.remove(task)
                        self._pending_tasks.remove_task(task)

        for task in tasks:
            if task.status == TaskStatus.Running:
                task.terminate(force=True)

            async with self._executed_tasks_lock.writer_lock:
                if task in self._executed_tasks:
                    self._executed_tasks.remove(task)

    async def terminate(self, task: AbstractTask, force: bool = False) -> None:
        if task.status == TaskStatus.Running:
            task.terminate(force=force)
