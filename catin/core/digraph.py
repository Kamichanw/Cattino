from collections import deque
import copy
from typing import Sequence, Tuple, TypeVar, Generic, List, Dict, Set

T = TypeVar("T")


class Node(Generic[T]):
    def __init__(self, value: T):
        self.data = value
        self.in_degree = 0
        self.out_degree = 0
        self.out_edges: List["Node[T]"] = []

    def add_out_edge(self, target: "Node[T]"):
        """Add an outgoing edge to another node"""
        if target not in self.out_edges:
            self.out_edges.append(target)
            self.out_degree += 1
            target.in_degree += 1

    def remove_out_edge(self, target: "Node[T]"):
        """Remove an outgoing edge to another node"""
        if target in self.out_edges:
            self.out_edges.remove(target)
            self.out_degree -= 1
            target.in_degree -= 1


class DiGraph(Generic[T]):
    def __init__(self):
        self._nodes: Dict[T, Node[T]] = {}
        self._node_set: Set[Node[T]] = set()

    def add_node(self, value: T):
        """Add a node to the DiGraph"""
        if value not in self._nodes:
            node = Node(value)
            self._nodes[value] = node
            self._node_set.add(node)

    def add_nodes_from(self, values: Sequence[T]):
        """Add multiple nodes to the DiGraph"""
        for value in values:
            self.add_node(value)

    def add_edge(self, u: T, v: T):
        """Add a directed edge u -> v to the DiGraph"""
        if u not in self._nodes:
            self.add_node(u)
        if v not in self._nodes:
            self.add_node(v)

        u_node = self._nodes[u]
        v_node = self._nodes[v]

        u_node.add_out_edge(v_node)

    def add_edges_from(self, edges: List[Tuple[T, T]]):
        """Add multiple directed edges to the DiGraph"""
        for u, v in edges:
            self.add_edge(u, v)

    def remove_edge(self, u: T, v: T):
        """Remove a directed edge u -> v from the DiGraph"""
        if u in self._nodes and v in self._nodes:
            u_node = self._nodes[u]
            v_node = self._nodes[v]
            u_node.remove_out_edge(v_node)

    def remove_edges_from(self, edges: List[Tuple[T, T]]):
        """Remove multiple directed edges from the DiGraph"""
        for u, v in edges:
            self.remove_edge(u, v)

    def remove_node(self, value: T):
        """Remove a node and all edges associated with it"""
        if value in self._nodes:
            node_to_remove = self._nodes[value]
            out_edges_to_remove = list(node_to_remove.out_edges)
            for target in out_edges_to_remove:
                node_to_remove.remove_out_edge(target)

            for node in self._node_set:
                node.remove_out_edge(node_to_remove)

            self._node_set.remove(node_to_remove)
            del self._nodes[value]

    def remove_nodes_from(self, values: List[T]):
        """Remove multiple nodes and all edges associated with them"""
        for value in values:
            self.remove_node(value)

    def clear(self):
        """Remove all nodes and edges from the DiGraph"""
        self._nodes.clear()
        self._node_set.clear()

    def merge(self, other_dag: "DiGraph[T]"):
        """Merge another DiGraph into this one, merging nodes and edges"""
        for node_value, node in other_dag._nodes.items():
            if node_value not in self._nodes:
                self.add_node(node_value)

            for target_node in node.out_edges:
                self.add_edge(node.data, target_node.data)

    @property
    def nodes(self) -> List[T]:
        """Return a list of all node values in the graph"""
        return [node.data for node in self._node_set]

    @property
    def graph(self) -> Dict[T, List[T]]:
        """Return the adjacency list (node -> [outgoing nodes])"""
        return {
            node.data: [target.data for target in node.out_edges]
            for node in self._node_set
        }

    @property
    def in_degree(self) -> Dict[T, int]:
        """Return the in-degree of each node in the graph"""
        return {node.data: node.in_degree for node in self._node_set}

    @property
    def out_degree(self) -> Dict[T, int]:
        """Return the out-degree of each node in the graph"""
        return {node.data: node.out_degree for node in self._node_set}

    def neighbors(self, value: T) -> List[T]:
        """Return a list of neighbors (outgoing nodes) of the given node"""
        if value in self._nodes:
            node = self._nodes[value]
            return [neighbor.data for neighbor in node.out_edges]
        else:
            return []

    def has_edge(self, u: T, v: T) -> bool:
        """Check if there is a directed edge u -> v"""
        if u in self._nodes and v in self._nodes:
            u_node = self._nodes[u]
            v_node = self._nodes[v]
            return v_node in u_node.out_edges
        return False

    def has_cycle(self) -> bool:
        """Check if the graph has a cycle with khan toposort"""
        in_degree = {node: node.in_degree for node in self._node_set}
        zero_in_degree = deque(
            node for node, degree in in_degree.items() if degree == 0
        )
        while zero_in_degree:
            node = zero_in_degree.popleft()
            for target in node.out_edges:
                in_degree[target] -= 1
                if in_degree[target] == 0:
                    zero_in_degree.append(target)
        return any(in_degree.values())

    def __len__(self) -> int:
        return len(self._nodes)

    def __iter__(self):
        return iter(self._nodes.keys())

    def __contains__(self, value: T) -> bool:
        return value in self._nodes

    def __deepcopy__(self, memo=None):
        """Create a deep copy of the DiGraph"""
        if memo is None:
            memo = {}

        new_graph = DiGraph[T]()
        for node_value, node in self._nodes.items():
            new_node = copy.deepcopy(node, memo)
            new_graph._nodes[node_value] = new_node
            new_graph._node_set.add(new_node)

        for node_value, node in new_graph._nodes.items():
            for target_node in node.out_edges:
                new_graph.add_edge(node.data, target_node.data)

        return new_graph
