import copy
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple, Union
from cattino.core.digraph import DiGraph
from cattino.tasks.interface import AbstractTask, Task, TaskGroup


class TaskGraph:
    """
    TaskGraph is used to specify the dependencies between tasks. Although it allows task groups to
    be added directly, each node within it still represents a single task.
    """

    def __init__(self):
        super().__init__()

        self._task_map: Dict[str, Task] = {}
        self._graph = DiGraph[Task]()

    @property
    def graph(self):
        return self._graph

    @property
    def in_degree(self):
        return self._graph.in_degree

    def add_task(self, task: AbstractTask):
        """Add a task or task group to the task graph."""
        if issubclass(type(task), TaskGroup):
            self.merge(task.graph)
        else:
            self._graph.add_node(task)
            self._task_map[task.name] = task

    def add_tasks_from(self, tasks: Sequence[AbstractTask]):
        """Add multiple tasks or task groups to the task graph."""
        for task in tasks:
            self.add_task(task)

    def add_edge(self, u: AbstractTask, v: AbstractTask):
        """
        Add a dependency edge from task u to task v, which means task u must be executed before task v.
        If u or v is a task group, all tasks from u will add edges to all tasks in v.
        """
        if issubclass(type(u), Task) and issubclass(type(v), Task):
            self._graph.add_edge(u, v)
        else:
            u_tasks = [u] if issubclass(type(u), Task) else u.all_tasks
            v_tasks = [v] if issubclass(type(v), Task) else v.all_tasks
            for ut in u_tasks:
                for vt in v_tasks:
                    self._graph.add_edge(ut, vt)

    def add_edges_from(
        self,
        edges: List[Tuple[AbstractTask, AbstractTask]],
    ):
        """Add multiple dependency edges to the task graph."""
        for u, v in edges:
            self.add_edge(u, v)

    def remove_task(self, task: AbstractTask):
        """Remove a task or all tasks from a group from the task graph."""
        tasks = task.all_tasks if issubclass(type(task), TaskGroup) else [task]
        for t in tasks:
            self._graph.remove_node(t)
            self._task_map.pop(t.name, None)

    def remove_tasks_from(self, tasks: List[AbstractTask]):
        """Remove multiple tasks from the task graph."""
        for task in tasks:
            self.remove_task(task)

    def remove_edge(self, u: AbstractTask, v: AbstractTask):
        """Remove a dependency edge from task u to task v."""
        if issubclass(type(u), Task) and issubclass(type(v), Task):
            self._graph.remove_edge(u, v)
        else:
            u_tasks = [u] if issubclass(type(u), Task) else u.all_tasks
            v_tasks = [v] if issubclass(type(v), Task) else v.all_tasks
            for ut in u_tasks:
                for vt in v_tasks:
                    self._graph.remove_edge(ut, vt)

    def remove_edges_from(
        self,
        edges: List[Tuple[AbstractTask, AbstractTask]],
    ):
        """Remove multiple dependency edges from the task graph."""
        for u, v in edges:
            self.remove_edge(u, v)

    def has_cycle(self):
        return self._graph.has_cycle()

    @property
    def tasks(self) -> List[Task]:
        """Return all tasks in the task graph."""
        return self._graph.nodes

    def get_successors(self, task: "Task") -> List["Task"]:
        return self._graph.neighbors(task)

    def merge(self, graph: "TaskGraph"):
        self._graph.merge(graph._graph)
        self._task_map.update(graph._task_map)

    def __len__(self):
        return len(self._graph)

    def __iter__(self):
        return iter(self._graph)

    def __contains__(self, item):
        return item in self._graph
