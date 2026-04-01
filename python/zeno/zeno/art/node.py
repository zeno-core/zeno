"""ART node implementations.

Implements the four node types of the Adaptive Radix Tree:
- Node4: Up to 4 children, linear search
- Node16: Up to 16 children, linear search (no SIMD in Python)
- Node48: Up to 48 children, 256-byte index array
- Node256: Up to 256 children, direct array access

Path compression: Each node stores up to 11 bytes of prefix inline.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import List, Optional, TYPE_CHECKING

from zeno.constants import (
    MAX_PREFIX_LEN,
    NODE4_CAPACITY,
    NODE16_CAPACITY,
    NODE48_CAPACITY,
    NODE256_CAPACITY,
)

if TYPE_CHECKING:
    from zeno.art.leaf import Leaf


class NodeType(Enum):
    """ART node types."""

    NODE4 = auto()
    NODE16 = auto()
    NODE48 = auto()
    NODE256 = auto()


class NodeHeader:
    """Common header for all ART internal nodes.

    Contains:
    - num_children: Current number of children
    - prefix_len: Length of compressed path prefix
    - prefix: Up to 11 bytes of inline prefix
    - leaf_value: Optional leaf value stored at this node (for exact prefix match)
    """

    __slots__ = ("num_children", "prefix_len", "node_type", "prefix", "leaf_value")

    def __init__(self, node_type: NodeType) -> None:
        self.num_children: int = 0
        self.prefix_len: int = 0
        self.node_type: NodeType = node_type
        self.prefix: bytearray = bytearray(MAX_PREFIX_LEN)
        self.leaf_value: Optional["Leaf"] = None


class Node4:
    """ART node with up to 4 children.

    Uses parallel arrays for keys and children, kept sorted
    to enable fast linear search and ordered iteration.
    """

    __slots__ = ("header", "keys", "children")

    def __init__(self) -> None:
        self.header = NodeHeader(NodeType.NODE4)
        self.keys: List[int] = [0] * NODE4_CAPACITY
        self.children: List[Optional["Leaf"]] = [None] * NODE4_CAPACITY

    @property
    def node_type(self) -> NodeType:
        return self.header.node_type

    @property
    def num_children(self) -> int:
        return self.header.num_children

    @property
    def prefix_len(self) -> int:
        return self.header.prefix_len

    @property
    def leaf_value(self) -> Optional["Leaf"]:
        return self.header.leaf_value

    @leaf_value.setter
    def leaf_value(self, value: Optional["Leaf"]) -> None:
        self.header.leaf_value = value

    def find_child(self, key_byte: int) -> Optional["Leaf"]:
        """Find child by key byte using linear search.

        Time Complexity: O(4) = O(1)
        """
        n = self.header.num_children
        for i in range(n):
            if self.keys[i] == key_byte:
                return self.children[i]
        return None

    def add_child(self, key_byte: int, child: "Leaf") -> None:
        """Add a child maintaining sorted order.

        Raises:
            Exception: If node is full
        """
        if self.header.num_children >= NODE4_CAPACITY:
            raise Exception("Node4 is full")

        count = self.header.num_children

        # Find insertion position (maintain sorted order)
        pos = 0
        while pos < count and self.keys[pos] < key_byte:
            pos += 1

        # Shift elements to make room
        for i in range(count, pos, -1):
            self.keys[i] = self.keys[i - 1]
            self.children[i] = self.children[i - 1]

        # Insert new child
        self.keys[pos] = key_byte
        self.children[pos] = child
        self.header.num_children = count + 1

    def remove_child(self, key_byte: int) -> None:
        """Remove a child by key byte."""
        n = self.header.num_children

        # Find position
        pos = 0
        while pos < n and self.keys[pos] != key_byte:
            pos += 1

        if pos >= n:
            return  # Not found

        # Shift elements to fill gap
        for i in range(pos, n - 1):
            self.keys[i] = self.keys[i + 1]
            self.children[i] = self.children[i + 1]

        self.header.num_children = n - 1


class Node16:
    """ART node with up to 16 children.

    Similar to Node4 but with 16 slots. Uses linear search
    since Python doesn't have SIMD.
    """

    __slots__ = ("header", "keys", "children")

    def __init__(self) -> None:
        self.header = NodeHeader(NodeType.NODE16)
        self.keys: List[int] = [0] * NODE16_CAPACITY
        self.children: List[Optional["Leaf"]] = [None] * NODE16_CAPACITY

    @property
    def node_type(self) -> NodeType:
        return self.header.node_type

    @property
    def num_children(self) -> int:
        return self.header.num_children

    @property
    def prefix_len(self) -> int:
        return self.header.prefix_len

    @property
    def leaf_value(self) -> Optional["Leaf"]:
        return self.header.leaf_value

    @leaf_value.setter
    def leaf_value(self, value: Optional["Leaf"]) -> None:
        self.header.leaf_value = value

    def find_child(self, key_byte: int) -> Optional["Leaf"]:
        """Find child by key byte using linear search.

        Time Complexity: O(16) = O(1)
        """
        n = self.header.num_children
        for i in range(n):
            if self.keys[i] == key_byte:
                return self.children[i]
        return None

    def add_child(self, key_byte: int, child: "Leaf") -> None:
        """Add a child maintaining sorted order.

        Raises:
            Exception: If node is full
        """
        if self.header.num_children >= NODE16_CAPACITY:
            raise Exception("Node16 is full")

        count = self.header.num_children

        # Find insertion position
        pos = 0
        while pos < count and self.keys[pos] < key_byte:
            pos += 1

        # Shift elements
        for i in range(count, pos, -1):
            self.keys[i] = self.keys[i - 1]
            self.children[i] = self.children[i - 1]

        self.keys[pos] = key_byte
        self.children[pos] = child
        self.header.num_children = count + 1

    def remove_child(self, key_byte: int) -> None:
        """Remove a child by key byte."""
        n = self.header.num_children

        pos = 0
        while pos < n and self.keys[pos] != key_byte:
            pos += 1

        if pos >= n:
            return

        for i in range(pos, n - 1):
            self.keys[i] = self.keys[i + 1]
            self.children[i] = self.children[i + 1]

        self.header.num_children = n - 1


class Node48:
    """ART node with up to 48 children.

    Uses a 256-byte index array for O(1) lookups.
    The index maps byte values (0-255) to child positions.
    """

    __slots__ = ("header", "child_index", "children", "present")

    EMPTY_INDEX = 255

    def __init__(self) -> None:
        self.header = NodeHeader(NodeType.NODE48)
        # Index: maps byte value to child position (255 = empty)
        self.child_index: List[int] = [self.EMPTY_INDEX] * 256
        # Children array (sparse, only 48 slots)
        self.children: List[Optional["Leaf"]] = [None] * NODE48_CAPACITY
        # Bitmap of used positions
        self.present: int = 0

    @property
    def node_type(self) -> NodeType:
        return self.header.node_type

    @property
    def num_children(self) -> int:
        return self.header.num_children

    @property
    def prefix_len(self) -> int:
        return self.header.prefix_len

    @property
    def leaf_value(self) -> Optional["Leaf"]:
        return self.header.leaf_value

    @leaf_value.setter
    def leaf_value(self, value: Optional["Leaf"]) -> None:
        self.header.leaf_value = value

    def _find_free_position(self) -> int:
        """Find first free position in children array using bit operations."""
        for i in range(NODE48_CAPACITY):
            if not (self.present & (1 << i)):
                return i
        raise Exception("Node48 has no free positions")

    def find_child(self, key_byte: int) -> Optional["Leaf"]:
        """Find child by key byte in O(1).

        Time Complexity: O(1)
        """
        idx = self.child_index[key_byte]
        if idx == self.EMPTY_INDEX:
            return None
        return self.children[idx]

    def add_child(self, key_byte: int, child: "Leaf") -> None:
        """Add a child.

        Raises:
            Exception: If node is full
        """
        if self.header.num_children >= NODE48_CAPACITY:
            raise Exception("Node48 is full")

        if self.child_index[key_byte] != self.EMPTY_INDEX:
            # Already exists, update
            idx = self.child_index[key_byte]
            self.children[idx] = child
            return

        # Find free position
        pos = self._find_free_position()

        self.child_index[key_byte] = pos
        self.children[pos] = child
        self.present |= 1 << pos
        self.header.num_children += 1

    def remove_child(self, key_byte: int) -> None:
        """Remove a child by key byte."""
        idx = self.child_index[key_byte]
        if idx == self.EMPTY_INDEX:
            return

        self.child_index[key_byte] = self.EMPTY_INDEX
        self.children[idx] = None
        self.present &= ~(1 << idx)
        self.header.num_children -= 1


class Node256:
    """ART node with up to 256 children.

    Direct array access for all possible byte values.
    Most memory-intensive but O(1) lookup.
    """

    __slots__ = ("header", "children")

    def __init__(self) -> None:
        self.header = NodeHeader(NodeType.NODE256)
        self.children: List[Optional["Leaf"]] = [None] * NODE256_CAPACITY

    @property
    def node_type(self) -> NodeType:
        return self.header.node_type

    @property
    def num_children(self) -> int:
        return self.header.num_children

    @property
    def prefix_len(self) -> int:
        return self.header.prefix_len

    @property
    def leaf_value(self) -> Optional["Leaf"]:
        return self.header.leaf_value

    @leaf_value.setter
    def leaf_value(self, value: Optional["Leaf"]) -> None:
        self.header.leaf_value = value

    def find_child(self, key_byte: int) -> Optional["Leaf"]:
        """Find child by key byte in O(1).

        Time Complexity: O(1)
        """
        return self.children[key_byte]

    def add_child(self, key_byte: int, child: "Leaf") -> None:
        """Add or update a child.

        Note: Node256 doesn't have a fixed capacity limit in theory,
        but we track num_children for consistency.
        """
        if self.children[key_byte] is None:
            self.header.num_children += 1
        self.children[key_byte] = child

    def remove_child(self, key_byte: int) -> None:
        """Remove a child by key byte."""
        if self.children[key_byte] is not None:
            self.children[key_byte] = None
            self.header.num_children -= 1
