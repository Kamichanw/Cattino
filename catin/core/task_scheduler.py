import asyncio
import re
import aiorwlock
from dataclasses import dataclass, field
from loguru import logger
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Dict,
    Sequence,
    Union,
    Optional,
)

from catin.tasks.interface import AbstractTask, Task, TaskGroup, TaskStatus
from catin.tasks.task_graph import TaskGraph
from catin.settings import settings


@dataclass
class GroupInfo:
    """
    This class is used to construct the relationship tree of tasks/task groups.
    A task or task group can belong to at most one larger task group, forming a tree structure.
    The purpose of the relationship tree is to ensure that the group hook is called correctly.
    """

    total: set = field(default_factory=set)  # total = done + remaining + running
    remaining: set = field(default_factory=set)
    done: set = field(default_factory=set)

    # a cache to check if the task is the first in the group
    # if the task is the first in the group, we will traverse up
    # the tree until we find the root task group or parent group_info.is_first is False
    # and call the on_task_group_start from top to bottom
    # NOTE: update this when the first task is resumed from suspended
    is_first: bool = True


class TaskScheduler:
    """
    Manages the execution of tasks, ensuring they are executed in the correct order and handling task dependencies.
    """

    def __init__(self) -> None:
        # when a task or a group is dispatched, it will be added to the _pending_tasks
        # and the task will be scheduled by self.step. A task can be started if:
        #       - it has no predecessors
        #       - it has the highest priority
        #       - it is created the earliest
        # only a task is finished, it will be removed from the pending tasks
        # we ensure that there is no overlap in the tasks or task groups from each dispatching
        self._pending_tasks = TaskGraph()
        self._pending_tasks_lock = aiorwlock.RWLock()

        # if a task starts, it will be added to the executed tasks
        self._executed_tasks: List[Task] = []
        self._executed_tasks_lock = aiorwlock.RWLock()

        # a dict to record the group info of a task to call on_task_group_* properly
        self._group_info: Dict[AbstractTask, GroupInfo] = {}
        self._group_info_lock = asyncio.Lock()

        self.outter_nodes = []

    def _remove_from_rel_tree(self, task: Task):
        """
        Remove task from task relation tree.
        Note that this method is lock-free, so caller should wrap it in pending task and group info locks.
        """
        if task not in self._group_info:
            # task is not in any group, return
            return
        group, group_info = task.group, self._group_info[task]

        # remove task from group info sets
        if task in group_info.total:
            group_info.total.remove(task)
        if task in group_info.done:
            group_info.done.remove(task)
        if task in group_info.remaining:
            group_info.remaining.remove(task)
        del self._group_info[task]

        if len(group_info.total) == 0:
            self._remove_from_rel_tree(group)

    async def step(self) -> bool:
        """
        Try to execute a waiting task if it is ready. Returns True if any tasks
        were executed, otherwise returns False.
        """

        def is_first(t: AbstractTask):
            """Whether given task or group is the first to start"""
            # check all subtasks, is_first can be True if
            #   - # remaining == # total
            #   - is_first of all subgroups are True
            assert t.group, f"{t.name} is not in any group"
            g = t.group
            group_info = self._group_info[t]
            return len(group_info.remaining) == len(group_info.total) and all(
                self._group_info[next(iter(sub_g))].is_first
                for sub_g in g.subtasks
                if issubclass(type(sub_g), TaskGroup)
            )

        def gather_pre_hook_tasks(task: AbstractTask):
            if task in self._group_info:
                group_info = self._group_info[task]
                group_info.is_first = is_first(task)
                if group_info.is_first:
                    return gather_pre_hook_tasks(task.group) + [task]
            return [task]

        def gather_post_hook_tasks(task: AbstractTask):
            if task in self._group_info:
                group_info = self._group_info[task]
                group_info.done.add(task)
                del self._group_info[task]
                if len(group_info.total) == len(group_info.done):
                    return [task] + gather_post_hook_tasks(task.group)
            return [task]

        async def execute(task: Task):
            if task in self._group_info:
                async with self._group_info_lock:
                    pre_hook_tasks = gather_pre_hook_tasks(task)
                    # we won't remove parent remaining set even there is no remaining
                    # task in current group, because user may re-add tasks to remaining set
                    # by suspending operation.
                    self._group_info[task].remaining.remove(task)
            else:
                pre_hook_tasks = [task]
            try:
                for t in pre_hook_tasks:
                    logger.info(
                        f"{'Group' if issubclass(type(t), TaskGroup) else 'Task'} {t.fullname} started."
                    )
                    t.on_start()
                task.start()
                await asyncio.to_thread(task.wait)
            except Exception as e:
                logger.exception(e)

            if task.status in [TaskStatus.Done, TaskStatus.Failed]:

                def cascade_cancel(task):
                    successors = self._pending_tasks.get_successors(task)
                    for t in successors:
                        cascade_cancel(t)
                        self._remove_from_rel_tree(t)
                        self._pending_tasks.remove_task(t)

                if (
                    settings.cascade_cancel_on_failure
                    and task.status == TaskStatus.Failed
                ):
                    async with self._pending_tasks_lock.writer_lock:
                        async with self._group_info_lock:
                            cascade_cancel(task)

                async with self._group_info_lock:
                    post_hook_tasks = gather_post_hook_tasks(task)

                async with self._pending_tasks_lock.writer_lock:
                    self._pending_tasks.remove_task(task)

                logger.info(f"Task {task.name} finished with status {task.status}")
                for t in post_hook_tasks:
                    logger.info(
                        f"{'Group' if issubclass(type(t), TaskGroup) else 'Task'} {t.name} ended."
                    )
                    t.on_end()

        # if not self.outter_nodes:
        #     async with self._executed_tasks_lock.reader_lock:
        #         async with self._pending_tasks_lock.reader_lock:
        #             outter_nodes = [
        #                 task
        #                 for task, ind in self._pending_tasks.in_degree.items()
        #                 if ind == 0 and task not in self._executed_tasks
        #             ]

        #     self.outter_nodes = sorted(
        #         outter_nodes,
        #         key=lambda task: (-task.priority, task.create_time),
        #     )

        # subset = self.outter_nodes[:5]
        # self.outter_nodes = self.outter_nodes[5:]
        # has_executed = False  # whether at least one task is executed

        # for upcoming_task in subset:
        #     if upcoming_task.is_ready:
        #         async with self._executed_tasks_lock.writer_lock:
        #             self._executed_tasks.append(upcoming_task)
        #         asyncio.create_task(execute(upcoming_task))
        #         has_executed = True

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

        async def try_create_task(upcoming_task):
            if upcoming_task.is_ready:
                async with self._executed_tasks_lock.writer_lock:
                    self._executed_tasks.append(upcoming_task)
                await execute(upcoming_task)

        asyncio.gather(*[try_create_task(task) for task in outter_nodes])

    async def get_task(self, task_name: str) -> Optional[Task]:
        async with self._executed_tasks_lock.reader_lock:
            for task in self._executed_tasks:
                if task.name == task_name:
                    return task
        async with self._pending_tasks_lock.reader_lock:
            return self._pending_tasks.get_task_by_name(task_name)

    @property
    async def running_tasks(self) -> List[Task]:
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
    async def all_tasks(self) -> List[Task]:
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
        task: AbstractTask,
    ) -> None:
        """
        Enqueue a task or a task group.
        """

        async def check_duplicate(tasks: Iterable[Task]):
            all_tasks_name = set(t.fullname for t in await self.all_tasks)
            duplicate_task_names = set(t.fullname for t in tasks) & all_tasks_name
            if duplicate_task_names:
                if settings.override_exist_tasks == "forbid":
                    raise ValueError(
                        f"Tasks with the same name can be executed only once, but got duplicate task names: {', '.join(duplicate_task_names)}. "
                        "Use `meow set override-exist-tasks allow` or `meow set override-exist-tasks rename` to suppress this error."
                    )
                elif settings.override_exist_tasks == "rename":
                    for t in tasks:
                        if t.fullname in duplicate_task_names:
                            m = re.match(r"(.+)_([0-9]+)$", t.fullname)
                            t.fullname = (
                                f"{t.name}_1"
                                if not m
                                else f"{m.group(1)}_{int(m.group(2)) + 1}"
                            )
                else:
                    await self.remove(
                        [await self.get_task(name) for name in duplicate_task_names]
                    )

        if issubclass(type(task), TaskGroup):
            await check_duplicate(task.all_tasks)

            def set_group_info(group: TaskGroup):
                group_info = GroupInfo(
                    total=set(group.subtasks),
                    remaining=set(group.subtasks),
                )
                for t in group.subtasks:
                    self._group_info[t] = group_info
                    if issubclass(type(t), TaskGroup):
                        set_group_info(t)

            set_group_info(task)
        else:
            await check_duplicate([task])

        async with self._pending_tasks_lock.writer_lock:
            self._pending_tasks.add_task(task)

    async def resume(self, task: Task) -> None:
        if task.status == TaskStatus.Suspended:
            task.resume()

    async def suspend(self, task: Task) -> None:
        if task.status in [TaskStatus.Running, TaskStatus.Waiting]:
            task.suspend()
            async with self._executed_tasks_lock.writer_lock:
                if task in self._executed_tasks:
                    self._executed_tasks.remove(task)
            async with self._group_info_lock:
                self._group_info[task].remaining.add(task)

    async def remove(self, tasks: Sequence[Task]) -> None:
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
                        async with self._group_info_lock:
                            self._remove_from_rel_tree(task)
                    self._pending_tasks.remove_task(task)

        for task in tasks:
            if task.status == TaskStatus.Running:
                task.terminate(force=True)

            async with self._executed_tasks_lock.writer_lock:
                if task in self._executed_tasks:
                    self._executed_tasks.remove(task)

    async def terminate(self, task: Task, force: bool = False) -> None:
        if task.status == TaskStatus.Running:
            task.terminate(force=force)
