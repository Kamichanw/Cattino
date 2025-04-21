from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, Optional, Dict, Union

T = TypeVar("T")  # Generic type for data


@dataclass
class Node(Generic[T]):
    """
    A node in the tree structure.

    Attributes:
        data (T, *optional*): The value of the node.
        children (dict): A dictionary mapping child names to their respective child nodes.
        parent (Node): The parent node of this node.
        name (str): The name of the node.
    """

    name: str
    data: Optional[T] = None
    children: Dict[str, "Node"] = field(default_factory=dict)
    parent: Optional["Node"] = None


class PathTree(Generic[T]):
    """
    A tree structure that supports add, delete, find, and modify operations
    based on a name path, separated by given separator.
    """

    def __init__(self, sep="/"):
        """
        Initializes a PathTree.

        Args:
            sep (str): The separator used for the path.
        """
        self._root = Node(name="root")
        self.nodes: Dict[str, Node] = {}
        self.sep: str = sep

    def set_node(self, path: str, value: T):
        """
        Set a node in the tree based on the given path.

        Args:
            path (str): The path from the root to the new node, separated by the specified separator.
            value (T): The value to set at the node.
        """
        parts = path.split(self.sep)
        current = self._root

        for i, part in enumerate(parts):
            if part not in current.children:
                current.children[part] = Node(
                    name=part, parent=current if current != self._root else None
                )
                self.nodes[self.sep.join(parts[:i + 1])] = current.children[part]
            current = current.children[part]
        current.data = value

    def del_node(self, path: str):
        """
        Deletes a node from the tree based on the given path.

        Args:
            path (str): The path from the root to the node, separated by the specified separator.
        """
        if path in self.nodes:
            node = self.nodes[path]
            if node.parent:
                del node.parent.children[node.name]
            del self.nodes[path]

    def get_node(self, path: str) -> Node:
        """
        Retrieves a node from the tree based on the given path or attribute.

        Args:
            path (str): The path from the root to the node, separated by the specified separator.

        Returns:
            Node: The node found at the specified path or attribute.
        """
        return self.nodes[path]

    def __getitem__(self, path: str) -> Optional[T]:
        """
        Retrieves the value of a node from the tree based on the given path.
        Args:
            path (str): The path from the root to the node, separated by the specified separator.
        Returns:
            T: The value found at the specified path.
        """
        return self.nodes[path].data

    def __setitem__(self, path: str, value: T):
        """
        Sets the value of a node in the tree based on the given path.

        Args:
            path (str): The path from the root to the node, separated by the specified separator.
            value (T): The value to set at the node.
        """
        self.set_node(path, value)

    def __contains__(self, path: str) -> bool:
        """
        Checks if a node exists in the tree based on the given path or attribute.

        Args:
            path (str): The path from the root to the node, separated by the specified separator.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        return path in self.nodes
