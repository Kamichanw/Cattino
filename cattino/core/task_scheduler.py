import asyncio
import re
import aiorwlock
from dataclasses import dataclass, field
from loguru import logger
from typing import (
    Any,
    Iterable,
    List,
    Dict,
    Sequence,
    Optional,
    overload,
)

from cattino.core.path_tree import PathTree
from cattino.tasks.interface import AbstractTask, Task, TaskGroup, TaskStatus
from cattino.tasks.task_graph import TaskGraph
from cattino.settings import settings


@dataclass
class GroupInfo:
    """
    GroupInfo is used to track the group info of a task or a task group.
    It is used to ensure the end hooks are called properly.
    """

    total: set = field(default_factory=set)  # total = done + remaining + running
    remaining: set = field(default_factory=set)
    done: set = field(default_factory=set)

    # a cache to check if the task is the first in the group
    # if the task is the first in the group, we will traverse up
    # the tree until we find the root task group or parent group_info.is_first is False
    # and call the on_start from top to bottom
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

        # group info ensures that the start/end hooks are called in the correct order
        self._group_info: Dict[AbstractTask, GroupInfo] = {}
        self._group_info_lock = asyncio.Lock()

        self._candidates: List[Task] = []

        self._name_tree = PathTree[AbstractTask]("fullname")
        self._name_tree_lock = aiorwlock.RWLock()

    def _remove_from_group_info(self, task: Task):
        """
        Remove task from group info tree.
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
            self._remove_from_group_info(group)

    async def _execute_task(self, task: Task):
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

            if task.status in [TaskStatus.Done, TaskStatus.Failed]:

                def cascade_cancel(task):
                    successors = self._pending_tasks.get_successors(task)
                    for t in successors:
                        cascade_cancel(t)
                        t.cancel()
                        self._remove_from_group_info(t)
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
        except Exception as e:
            logger.exception(e)

    async def step(self):
        """
        Try to execute a waiting task if it is ready. Returns True if any tasks
        were executed, otherwise returns False.
        """

        if not self._candidates:
            async with self._executed_tasks_lock.reader_lock:
                async with self._pending_tasks_lock.reader_lock:
                    outter_nodes = [
                        task
                        for task, ind in self._pending_tasks.in_degree.items()
                        if ind == 0 and task not in self._executed_tasks
                    ]
            self._candidates = sorted(
                outter_nodes,
                key=lambda task: (-task.priority, task.create_time),
            )

        upcoming_task = self._candidates[0] if self._candidates else None
        if upcoming_task and upcoming_task.is_ready:
            self._candidates = self._candidates[1:]
            async with self._executed_tasks_lock.writer_lock:
                self._executed_tasks.append(upcoming_task)
            asyncio.create_task(self._execute_task(upcoming_task))
            return True
        return False

    @overload
    async def get_task(self, fullname: str) -> Optional[AbstractTask]:
        """Get a task or group by its fullname."""
        ...

    @overload
    async def get_task(self, pattern: re.Pattern) -> Optional[AbstractTask]:
        """Get a task or group by its name pattern."""
        ...

    async def get_task(self, fullname_or_pattern) -> Optional[AbstractTask]:
        async with self._name_tree_lock.reader_lock:
            if isinstance(fullname_or_pattern, re.Pattern):
                for fullname in self._name_tree.nodes:
                    if fullname_or_pattern.search(fullname):
                        return self._name_tree[fullname]
                return None
            if isinstance(fullname_or_pattern, str):
                try:
                    return self._name_tree[fullname_or_pattern]
                except KeyError:
                    return None

        raise TypeError(
            f"fullname must be str or re.Pattern, but got {type(fullname_or_pattern)}"
        )

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
    async def pending_tasks(self) -> List[Task]:
        """
        Get all pending tasks.
        """
        async with self._pending_tasks_lock.reader_lock:
            return [
                task
                for task in self._pending_tasks.tasks
                if task.status in [TaskStatus.Waiting, TaskStatus.Suspended]
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
            """
            Check if there are duplicate tasks in the given tasks. Returns
            the duplicate tasks that need to be removed.
            """
            duplicate_task_names = set(t.fullname for t in tasks) & set(
                t.fullname for t in await self.all_tasks
            )
            if duplicate_task_names:
                if settings.override_exist_tasks == "forbid":
                    raise ValueError(
                        f"Tasks with the same name can be executed only once, but got duplicate task names: {', '.join(duplicate_task_names)}. "
                        "Use `meow set override-exist-tasks allow` or `meow set override-exist-tasks rename` to suppress this error."
                    )
                elif settings.override_exist_tasks == "rename":
                    # TODO: Add relation tree to accelerate name matching
                    for t in tasks:
                        if t.fullname in duplicate_task_names:
                            m = re.match(r"(.+)_([0-9]+)$", t.name)
                            t.name = (
                                f"{t.name}_1"
                                if not m
                                else f"{m.group(1)}_{int(m.group(2)) + 1}"
                            )
                elif settings.override_exist_tasks == "allow":
                    await self.remove(
                        [await self.get_task(name) for name in duplicate_task_names]
                    )
                else:
                    return duplicate_task_names
            return []

        if issubclass(type(task), TaskGroup):
            name_to_skip = await check_duplicate(task.all_tasks)

            def set_group_info(group: TaskGroup):
                group_info = GroupInfo(
                    total=set(group.subtasks),
                    remaining=set(group.subtasks),
                )
                for t in group.subtasks:
                    self._group_info[t] = group_info
                    if issubclass(type(t), TaskGroup):
                        set_group_info(t)
                    else:
                        if t.fullname in name_to_skip:
                            t.cancel()
                            logger.info(
                                f"Task {t.name} already exists, skipping dispatch."
                            )
                    self._name_tree[t.fullname] = t

            async with self._name_tree_lock.writer_lock:
                set_group_info(task)
        else:
            name_to_skip = await check_duplicate([task])
            if name_to_skip == [task.fullname]:
                task.cancel()
                logger.info(f"Task {task.name} already exists, skipping dispatch.")
                return
            async with self._name_tree_lock.writer_lock:
                self._name_tree[task.fullname] = task

        async with self._pending_tasks_lock.writer_lock:
            self._pending_tasks.add_task(task)

    async def resume(self, task: AbstractTask) -> None:
        task.resume()

    async def suspend(self, task: AbstractTask) -> None:
        task.suspend()
        tasks = task.all_tasks if issubclass(type(task), TaskGroup) else [task]
        suspended_tasks = [t for t in tasks if t.status == TaskStatus.Suspended]
        async with self._executed_tasks_lock.writer_lock:
            for t in suspended_tasks:
                if t in self._executed_tasks:
                    self._executed_tasks.remove(t)
        async with self._group_info_lock:
            for t in suspended_tasks:
                self._group_info[t].remaining.add(t)

    async def remove(self, tasks: Sequence[AbstractTask]) -> None:
        """
        Remove a task from the pending tasks or executed tasks.
        If the task is running, terminate it forcefully.
        """
        # we must remove the task from the pending tasks first
        # to ensure the on_end of groups is called properly
        for task in tasks:
            async with self._pending_tasks_lock.writer_lock:
                if task in self._pending_tasks.tasks:
                    if task.status in [TaskStatus.Waiting, TaskStatus.Suspended]:
                        async with self._group_info_lock:
                            self._remove_from_group_info(task)
                    self._pending_tasks.remove_task(task)

        for task in tasks:
            if task.status == TaskStatus.Running:
                task.terminate(force=True)

            async with self._executed_tasks_lock.writer_lock:
                if task in self._executed_tasks:
                    self._executed_tasks.remove(task)

    async def terminate(self, task: AbstractTask, force: bool = False) -> None:
        task.terminate(force=force)
