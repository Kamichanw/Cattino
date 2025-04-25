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
        # when a task or a group is dispatched, it will be added to the _task_graph
        # and the task will be scheduled by self.step. A task can be started if:
        #       - it has no predecessors
        #       - it has the highest priority
        #       - it is created the earliest
        # we ensure that there is no overlap in the tasks or task groups from each dispatching
        self._task_graph = TaskGraph()
        self._task_graph_lock = aiorwlock.RWLock()

        # group info ensures that the start/end hooks are called in the correct order
        self._group_info: Dict[AbstractTask, GroupInfo] = {}
        self._group_info_lock = asyncio.Lock()

        self._name_tree = PathTree[AbstractTask]("fullname")
        self._name_tree_lock = aiorwlock.RWLock()

    def _remove_from_group_info(self, task: AbstractTask):
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

        if len(group_info.total) == 0 and group:
            self._remove_from_group_info(group)

    async def _execute_task(self, task: Task):
        def is_first(t: AbstractTask):
            """Whether given task or group is the first to start"""
            # check all subtasks, is_first can be True if
            #   - # remaining == # total
            #   - is_first of all subgroups are True
            assert t.group, f"{t.fullname} is not in any group"
            g = t.group
            group_info = self._group_info[t]
            return len(group_info.remaining) == len(group_info.total) and all(
                self._group_info[next(iter(sub_g))].is_first  # type: ignore
                for sub_g in g.subtasks
                if issubclass(type(sub_g), TaskGroup)
            )

        def gather_pre_hook_tasks(task: AbstractTask):
            if task in self._group_info:
                group_info = self._group_info[task]
                group_info.is_first = is_first(task)
                if group_info.is_first and task.group:
                    return gather_pre_hook_tasks(task.group) + [task]
            return [task]

        def gather_post_hook_tasks(task: AbstractTask):
            if task in self._group_info:
                group_info = self._group_info[task]
                group_info.done.add(task)
                del self._group_info[task]
                if len(group_info.total) == len(group_info.done) and task.group:
                    return [task] + gather_post_hook_tasks(task.group)
            return [task]

        try:
            if task in self._group_info:
                async with self._group_info_lock:
                    pre_hook_tasks = gather_pre_hook_tasks(task)
                    # we won't remove parent remaining set even there is no remaining
                    # task in current group, because user may re-add tasks to remaining set
                    # by cancelling operation.
                    self._group_info[task].remaining.remove(task)
            else:
                pre_hook_tasks = [task]

            for t in pre_hook_tasks:
                logger.info(
                    f"{'Group' if issubclass(type(t), TaskGroup) else 'Task'} {t.fullname} started."
                )
                t.on_start()
            task.start()
            await asyncio.to_thread(task.wait)

            if task.status in [TaskStatus.Done, TaskStatus.Failed]:

                def cascade_cancel(task):
                    for t in self._task_graph.neighbors(task):
                        cascade_cancel(t)
                        t.cancel()
                        self._remove_from_group_info(t)

                if (
                    settings.cascade_cancel_on_failure
                    and task.status == TaskStatus.Failed
                ):
                    async with self._task_graph_lock.writer_lock:
                        async with self._group_info_lock:
                            cascade_cancel(task)

                async with self._group_info_lock:
                    post_hook_tasks = gather_post_hook_tasks(task)

                logger.info(f"Task {task.fullname} finished with status {task.status}")
                for t in post_hook_tasks:
                    logger.info(
                        f"{'Group' if issubclass(type(t), TaskGroup) else 'Task'} {t.fullname} ended."
                    )
                    t.on_end()
        except Exception as e:
            logger.exception(e)
        finally:
            async with self._task_graph_lock.writer_lock:
                if task in self._task_graph.tasks:
                    self._task_graph.nx_graph.nodes[task]["started"] = False

    @property
    async def is_running(self) -> bool:
        """
        Check if the task scheduler is running.
        """
        async with self._task_graph_lock.reader_lock:
            return len(self._task_graph) != 0 and any(
                self._task_graph.nx_graph.nodes[task].get("started", False)
                for task in self._task_graph.tasks
            )

    async def step(self):
        """
        Try to execute a waiting task if it is ready. Returns True if any tasks
        were executed, otherwise returns False.
        """

        async with self._task_graph_lock.reader_lock:
            avail_task_graph = self._task_graph.filter_tasks(
                lambda task: task.status
                not in [TaskStatus.Done, TaskStatus.Failed, TaskStatus.Running]
                and not self._task_graph.nx_graph.nodes[task].get("started", False)
            )
            outter_nodes: List[Task] = [
                task
                for task in avail_task_graph.nodes
                if avail_task_graph.in_degree(task) == 0
            ]

        upcoming_task = min(
            outter_nodes,
            key=lambda task: (-task.priority, task.create_time),
            default=None,
        )

        logger.debug(
            f"Upcoming task: {upcoming_task.fullname if upcoming_task else 'None'}"
        )

        if upcoming_task and upcoming_task.is_ready:
            async with self._task_graph_lock.writer_lock:
                self._task_graph.nx_graph.nodes[upcoming_task]["started"] = True
            asyncio.create_task(self._execute_task(upcoming_task))
            return True
        return False

    @overload
    async def get_tasks(self, fullname: str) -> Optional[List[Task]]:
        """Get tasks by its fullname."""
        ...

    @overload
    async def get_tasks(self, pattern: re.Pattern[str]) -> Optional[List[Task]]:
        """Get tasks by its name pattern."""
        ...

    async def get_tasks(self, fullname_or_pattern) -> Optional[List[Task]]:  # type: ignore
        if isinstance(fullname_or_pattern, re.Pattern):
            return [
                t
                for t in await self.all_tasks
                if fullname_or_pattern.search(t.fullname)
            ] or None

        if isinstance(fullname_or_pattern, str):
            async with self._name_tree_lock.reader_lock:
                try:
                    selected_tasks = self._name_tree[fullname_or_pattern]
                    return [selected_tasks] if issubclass(type(selected_tasks), Task) else selected_tasks.all_tasks  # type: ignore
                except KeyError:
                    return None

        raise TypeError(
            f"fullname must be str or re.Pattern, but got {type(fullname_or_pattern)}"
        )

    async def get_task_object(self, fullname: str) -> Optional[AbstractTask]:
        """
        Get task object by its fullname.
        """
        async with self._name_tree_lock.reader_lock:
            try:
                return self._name_tree[fullname]
            except KeyError:
                return None

    @property
    async def all_tasks(self) -> List[Task]:
        """
        Get all tasks, including executed and pending tasks.
        """
        async with self._task_graph_lock.reader_lock:
            return self._task_graph.tasks

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
            if duplicate_task_names := set(t.fullname for t in tasks) & set(
                t.fullname for t in await self.all_tasks
            ):
                if settings.override_exist_tasks == "forbid":
                    raise ValueError(
                        f"Tasks with the same name can be executed only once, but got duplicate task names: {', '.join(duplicate_task_names)}. "
                        "Use `meow set override-exist-tasks allow` or `meow set override-exist-tasks rename` to suppress this error."
                    )
                elif settings.override_exist_tasks == "rename":
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
                        [self._name_tree[name] for name in duplicate_task_names]  # type: ignore
                    )

        if issubclass(type(task), TaskGroup):
            await check_duplicate(task.all_tasks)  # type: ignore

            def set_group_info(group: TaskGroup):
                group_info = GroupInfo(
                    total=set(group.subtasks),
                    remaining=set(group.subtasks),
                )
                for t in group.subtasks:
                    self._group_info[t] = group_info
                    if issubclass(type(t), TaskGroup):
                        set_group_info(t)  # type: ignore
                    else:
                        self._name_tree[t.fullname] = t
                self._name_tree[group.fullname] = group

            # group info lock is no need here
            async with self._name_tree_lock.writer_lock:
                set_group_info(task)  # type: ignore
        else:
            await check_duplicate([task])  # type: ignore

            async with self._name_tree_lock.writer_lock:
                self._name_tree[task.fullname] = task

        async with self._task_graph_lock.writer_lock:
            self._task_graph.add_task(task)

    async def resume(self, task: Task) -> None:
        task.resume()

    async def cancel(self, task: Task) -> None:
        task.cancel()
        if task.status == TaskStatus.Cancelled:
            async with self._task_graph_lock.writer_lock:
                self._task_graph.nx_graph.nodes[task]["started"] = False
            async with self._group_info_lock:
                self._group_info[task].remaining.add(task)

    async def remove(self, tasks: Sequence[Task]) -> None:
        """
        Remove a task from the pending tasks or executed tasks.
        If the task is running, terminate it forcefully.
        """
        # we must remove the task from the pending tasks first
        # to ensure the on_end of groups is called properly
        for task in tasks:
            async with self._task_graph_lock.writer_lock:
                if task in self._task_graph.tasks:
                    if task.status in [TaskStatus.Waiting, TaskStatus.Cancelled]:
                        async with self._group_info_lock:
                            self._remove_from_group_info(task)
                    self._task_graph.remove_task(task)

        for task in tasks:
            if task.status == TaskStatus.Running:
                task.terminate(force=True)

            async with self._task_graph_lock.writer_lock:
                self._task_graph.remove_task(task)

    async def terminate(self, task: AbstractTask, force: bool = False) -> None:
        task.terminate(force=force)
