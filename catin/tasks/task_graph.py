import copy
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple
from catin.core.digraph import DiGraph

if TYPE_CHECKING:
    from catin.tasks.interface import AbstractTask


class TaskGraph:
    def __init__(self):
        super().__init__()

        # map from task name to task
        self._graph = DiGraph["AbstractTask"]()
        self._task_map: Dict[str, "AbstractTask"] = {}
        self._name_graph = DiGraph[str]()

    def get_task_by_name(self, task_name: str) -> Optional["AbstractTask"]:
        return self._task_map.get(task_name, None)
    
    def get_successors(self, task: "AbstractTask") -> List["AbstractTask"]:
        return self._graph.neighbors(task)

    @property
    def in_degree(self):
        return self._graph.in_degree

    def add_task(self, task: "AbstractTask"):
        """Add a task to the task graph."""
        self._graph.add_node(task)
        self._name_graph.add_node(task.name)
        self._task_map[task.name] = task

    def add_tasks_from(self, tasks: Sequence["AbstractTask"]):
        """Add multiple tasks to the task graph."""
        self._graph.add_nodes_from(tasks)
        self._name_graph.add_nodes_from([task.name for task in tasks])
        self._task_map.update({task.name: task for task in tasks})


    def add_edge(self, u: "AbstractTask", v: "AbstractTask"):
        """
        Add a dependency edge from task u to task v, which means task u must be executed before task v.
        """
        self._graph.add_edge(u, v)
        self._name_graph.add_edge(u.name, v.name)

    def add_edges_from(self, edges: List[Tuple["AbstractTask", "AbstractTask"]]):
        """Add multiple dependency edges to the task graph."""
        self._graph.add_edges_from(edges)
        self._name_graph.add_edges_from([(u.name, v.name) for u, v in edges])

    def remove_task(self, task: "AbstractTask"):
        """Remove a task from the task graph."""
        self._graph.remove_node(task)
        self._name_graph.remove_node(task.name)
        self._task_map.pop(task.name, None) # supress KeyError if task is not in the map

    def remove_tasks_from(self, tasks: List["AbstractTask"]):
        """Remove multiple tasks from the task graph."""
        self._graph.remove_nodes_from(tasks)
        self._name_graph.remove_nodes_from([task.name for task in tasks])
        for task in tasks:
            self._task_map.pop(task.name, None)

    def remove_edge(self, u: "AbstractTask", v: "AbstractTask"):
        """Remove a dependency edge from task u to task v."""
        self._graph.remove_edge(u, v)
        self._name_graph.remove_edge(u.name, v.name)

    def remove_edges_from(self, edges: List[Tuple["AbstractTask", "AbstractTask"]]):
        """Remove multiple dependency edges from the task graph."""
        self._graph.remove_edges_from(edges)
        self._name_graph.remove_edges_from([(u.name, v.name) for u, v in edges])

    def merge(self, other_graph: "TaskGraph"):
        """Merge another task graph into this task graph."""
        self._graph.merge(other_graph._graph)
        self._name_graph.merge(other_graph._name_graph)
        self._task_map.update(other_graph._task_map)

    def test_cycle(self, other_graph: "TaskGraph") -> bool:
        """Check if adding another task graph would create a cycle."""
        name_graph = copy.deepcopy(self._name_graph)
        name_graph.merge(other_graph._name_graph)
        return name_graph.has_cycle()

    @property
    def tasks(self):
        return self._graph.nodes

    def __iter__(self):
        return iter(self._graph)

    def __len__(self):
        return len(self._graph)
