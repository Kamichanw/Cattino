from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, Optional, Dict, Union

T = TypeVar("T")  # Generic type for data


@dataclass
class Node:
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

    def __init__(self, *attr_mapping, sep="/"):
        """
        Initializes a PathTree with optional attr mapping.

        Args:
            *attr_mapping: The name of attributes in T that will be used for creating
                attr to node mappings. The type of given attributes cannot be str and
                must be hashable.
        """
        self._root = Node(name="root")
        self._attr_mapping = attr_mapping or ()
        self._rev_mapping: Dict[Any, Node] = {}
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

        for part in parts:
            if part not in current.children:
                current.children[part] = Node(
                    name=part, parent=current if current != self._root else None
                )
            current = current.children[part]

        current.data = value

        for attr in self._attr_mapping:
            if hasattr(value, attr):
                attr_value = getattr(value, attr)
                if isinstance(attr_value, str):
                    raise TypeError(
                        f"Attribute '{attr}' of value '{attr_value}' cannot be str."
                    )
                self._rev_mapping[attr_value] = current

    def del_node(self, path_or_attr: Union[str, Any]):
        """
        Deletes a node from the tree based on the given path or attribute.

        Args:
            path_or_attr (str or Any): The path from the root to the node, separated by the specified separator,
                or an attribute value of the node to delete.
        """
        if isinstance(path_or_attr, str):
            path = path_or_attr
            parts = path.split(self.sep)
            current = self._root
            parent = None
            key_to_delete = None

            for part in parts:
                if part in current.children:
                    parent = current
                    key_to_delete = part
                    current = current.children[part]
                else:
                    raise KeyError(f"Path '{path}' not found in the tree.")

            if key_to_delete:
                for attr in self._attr_mapping:
                    if hasattr(current.data, attr):
                        del self._rev_mapping[getattr(current.data, attr)]
                del parent.children[key_to_delete]
        else:
            attr_value = path_or_attr
            if attr_value not in self._rev_mapping:
                raise KeyError(f"Attribute '{attr_value}' not found in the tree.")
            node_to_delete = self._rev_mapping[attr_value]
            parent = node_to_delete.parent
            if parent:
                del parent.children[node_to_delete.name]
            del self._rev_mapping[attr_value]

    def get_node(self, path_or_attr: Union[str, Any]) -> Node:
        """
        Retrieves a node from the tree based on the given path or attribute.

        Args:
            path_or_attr (str or Any): The path from the root to the node, separated by the specified separator,
                or an attribute value of the node to retrieve.

        Returns:
            Node: The node found at the specified path or attribute.
        """
        if isinstance(path_or_attr, str):
            path = path_or_attr
            parts = path.split(self.sep)
            current = self._root

            for part in parts:
                if part in current.children:
                    current = current.children[part]
                else:
                    raise KeyError(f"Path '{path}' not found in the tree.")

            return current
        else:
            attr_value = path_or_attr
            if attr_value not in self._rev_mapping:
                raise KeyError(f"Attribute '{attr_value}' not found in the tree.")
            return self._rev_mapping[attr_value]

    def __getitem__(self, path_or_attr: Union[str, Any]) -> Optional[T]:
        """
        Retrieves the value of a node from the tree based on the given path or attribute.
        Args:
            path_or_attr (str or Any): The path from the root to the node, separated by the specified separator,
                or an attribute value of the node to retrieve.
        Returns:
            T: The value found at the specified path or attribute.
        """
        return self.get_node(path_or_attr).data

    def __contains__(self, path_or_attr: Union[str, Any]) -> bool:
        """
        Checks if a node exists in the tree based on the given path or attribute.

        Args:
            path_or_attr (str or Any): The path from the root to the node, separated by the specified separator,
                or an attribute value of the node to check.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        try:
            self.get_node(path_or_attr)
            return True
        except KeyError:
            return False
