import networkx as nx
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Tuple, Union
from cattino.tasks.interface import AbstractTask, Task, TaskGroup


class TaskGraph:
    """
    TaskGraph is used to specify the dependencies between tasks. Although it allows task groups to
    be added directly, each node within it still represents a single task.
    """

    def __init__(self):
        super().__init__()
        self._graph = nx.DiGraph()

    @property
    def nx_graph(self):
        return self._graph

    def neighbors(self, task: Task) -> List[Task]:
        """
        Get the neighbors of a task in the graph.
        """
        return list(self._graph.neighbors(task))

    def filter_tasks(self, predicate: Callable[[Task], bool]) -> nx.DiGraph:
        """
        Filter tasks in the graph based on a predicate function.
        The predicate function should take a task as input and return True or False.

        Args:
            predicate (Callable): A function taking a task as input, which returns `True` if the task
                should appear in the subgraph.
        
        Returns:
            nx.DiGraph: A read-only graph view of the task graph, filtered by the predicate.
        """
        return nx.subgraph_view( # type: ignore
            self._graph,
            filter_node=predicate,
        )

    def add_task(self, task: AbstractTask):
        """Add a task or task group to the task graph."""
        if issubclass(type(task), TaskGroup):
            self._graph.add_nodes_from(task.all_tasks)  # type: ignore
        else:
            self._graph.add_node(task)  # type: ignore

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
            self._graph.add_edge(u, v)  # type: ignore
        else:
            u_tasks = [u] if issubclass(type(u), Task) else u.all_tasks  # type: ignore
            v_tasks = [v] if issubclass(type(v), Task) else v.all_tasks  # type: ignore
            self._graph.add_edges_from([(ut, vt) for ut in u_tasks for vt in v_tasks])

    def add_edges_from(self, edges: List[Tuple[AbstractTask, AbstractTask]]):
        """Add multiple dependency edges to the task graph."""
        for u, v in edges:
            self.add_edge(u, v)

    def remove_task(self, task: AbstractTask):
        """Remove a task or all tasks from a group from the task graph."""
        tasks = task.all_tasks if issubclass(type(task), TaskGroup) else [task]  # type: ignore
        self._graph.remove_nodes_from(tasks)

    def remove_tasks_from(self, tasks: List[AbstractTask]):
        """Remove multiple tasks from the task graph."""
        for task in tasks:
            self.remove_task(task)

    def remove_edge(self, u: AbstractTask, v: AbstractTask):
        """Remove a dependency edge from task u to task v."""
        if issubclass(type(u), Task) and issubclass(type(v), Task):
            self._graph.remove_edge(u, v)  # type: ignore
        else:
            u_tasks = [u] if issubclass(type(u), Task) else u.all_tasks  # type: ignore
            v_tasks = [v] if issubclass(type(v), Task) else v.all_tasks  # type: ignore
            for ut in u_tasks:
                for vt in v_tasks:
                    self._graph.remove_edge(ut, vt)

    def remove_edges_from(self, edges: List[Tuple[AbstractTask, AbstractTask]]):
        """Remove multiple dependency edges from the task graph."""
        for u, v in edges:
            self.remove_edge(u, v)

    def has_cycle(self):
        return not nx.is_directed_acyclic_graph(self._graph)

    @property
    def tasks(self) -> List[Task]:
        """Return all tasks in the task graph."""
        return list(self._graph.nodes)

    def merge(self, graph: "TaskGraph"):
        # Merge another TaskGraph into this one using the `networkx` graph
        self._graph.update(graph.nx_graph)

    def __len__(self):
        return len(self._graph)

    def __iter__(self):
        return iter(self._graph)

    def __contains__(self, item):
        return item in self._graph
