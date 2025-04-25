from dataclasses import dataclass, field
from typing import Generic, TypeVar, Optional, Dict

T = TypeVar("T")


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
    sep: str
    data: Optional[T] = None
    children: Dict[str, "Node"] = field(default_factory=dict)
    parent: Optional["Node"] = None

    def __str__(self) -> str:
        """
        A string representation of the node, including its name and children.
        """
        if self.children:
            if len(self.children) > 1:
                return f"{self.name}{self.sep}{{{', '.join(str(child) for child in self.children.values())}}}"
            else:
                return f"{self.name}{self.sep}{next(iter(self.children.values()))}"
        return self.name


class PathTree(Generic[T]):
    """
    A tree structure that supports add, delete, find, and modify operations
    based on a name path, separated by given separator.
    """

    def __init__(self, sep: str = "/"):
        """
        Initializes a PathTree.

        Args:
            sep (str): The separator used for the path.
        """
        self._root = Node(name="root", sep=sep)
        self.nodes: Dict[str, Node] = {}
        self.sep: str = sep
    
    @property
    def roots(self) -> Dict[str, Node]:
        """
        Returns the roots of the tree.

        Returns:
            Dict[str, Node]: A dictionary of root children nodes.
        """
        return self._root.children

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
                    name=part,
                    sep=self.sep,
                    parent=current if current != self._root else None,
                )
                self.nodes[self.sep.join(parts[: i + 1])] = current.children[part]
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
        return self.nodes[path].data

    def __setitem__(self, path: str, value: T):
        self.set_node(path, value)

    def __contains__(self, path: str) -> bool:
        return path in self.nodes

    def __str__(self) -> str:
        return (
            ", ".join([str(node) for node in self._root.children.values()])
            if self._root.children
            else ""
        )
