"""ART Tree implementation.

This is the main index structure for Zeno, providing:
- O(k) insert, lookup, and delete where k is key length
- Prefix scan for range queries
- Path compression for space efficiency
- Adaptive node sizes for performance
"""

from __future__ import annotations

from typing import List, Optional, Tuple, Union

from zeno.art.node import Node4, Node16, Node48, Node256, NodeType, NodeHeader
from zeno.art.leaf import Leaf
from zeno.types import Value
from zeno.constants import MAX_PREFIX_LEN


# Union type for all node types
Node = Union[Node4, Node16, Node48, Node256]


class Tree:
    """Adaptive Radix Tree index.

    Provides ordered key-value storage with efficient:
    - Point operations (insert, lookup, delete)
    - Range operations (prefix scan, range scan)
    """

    __slots__ = ("root", "_size")

    def __init__(self) -> None:
        """Initialize an empty ART."""
        self.root: Optional[Union[Node, Leaf]] = None
        self._size: int = 0

    def is_empty(self) -> bool:
        """Check if tree is empty."""
        return self.root is None

    def size(self) -> int:
        """Return number of keys in tree."""
        return self._size

    def _match_prefix(self, node: Node, key: bytes, depth: int) -> Optional[int]:
        """Match compressed prefix of node against key.

        Args:
            node: Current node
            key: Key being searched
            depth: Current depth in key

        Returns:
            New depth if all prefix bytes match, None otherwise
        """
        header = node.header
        prefix_len = header.prefix_len

        # Check stored prefix (up to MAX_PREFIX_LEN bytes)
        max_cmp = min(prefix_len, MAX_PREFIX_LEN)
        for i in range(max_cmp):
            if depth >= len(key) or header.prefix[i] != key[depth]:
                return None
            depth += 1

        # If prefix is longer than MAX_PREFIX_LEN, check hidden part
        # by comparing with a leaf's key
        if prefix_len > MAX_PREFIX_LEN:
            leaf = self._find_any_leaf(node)
            limit = min(len(leaf.key), len(key))
            for i in range(MAX_PREFIX_LEN, prefix_len):
                if depth >= limit or leaf.key[depth] != key[depth]:
                    return None
                depth += 1

        return depth

    def _find_any_leaf(self, node: Node) -> Leaf:
        """Find any leaf in subtree (for prefix comparison)."""
        current: Node = node

        while True:
            if current.header.leaf_value is not None:
                return current.header.leaf_value

            # Get first child
            if isinstance(current, Node4):
                if current.num_children > 0:
                    child = current.children[0]
                    if isinstance(child, Leaf):
                        return child
                    current = child
                else:
                    raise RuntimeError("Node has no children or leaf")
            elif isinstance(current, Node16):
                if current.num_children > 0:
                    child = current.children[0]
                    if isinstance(child, Leaf):
                        return child
                    current = child
                else:
                    raise RuntimeError("Node has no children or leaf")
            elif isinstance(current, Node48):
                # Find first valid child
                for i in range(256):
                    child = current.find_child(i)
                    if child is not None:
                        if isinstance(child, Leaf):
                            return child
                        current = child
                        break
                else:
                    raise RuntimeError("Node has no children")
            elif isinstance(current, Node256):
                # Find first valid child
                for i in range(256):
                    child = current.find_child(i)
                    if child is not None:
                        if isinstance(child, Leaf):
                            return child
                        current = child
                        break
                else:
                    raise RuntimeError("Node has no children")

    def lookup(self, key: bytes) -> Optional[Value]:
        """Lookup value by key.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to lookup

        Returns:
            Value if found, None otherwise
        """
        if self.root is None:
            return None

        # Handle root being a leaf
        if isinstance(self.root, Leaf):
            if self.root.key == key:
                return self.root.value
            return None

        current = self.root
        depth = 0

        while True:
            # Check prefix
            new_depth = self._match_prefix(current, key, depth)
            if new_depth is None:
                return None
            depth = new_depth

            # Check if we're at the end of the key
            if depth == len(key):
                leaf = current.header.leaf_value
                if leaf is not None and leaf.key == key:
                    return leaf.value
                return None

            # Follow child pointer
            key_byte = key[depth]
            child = current.find_child(key_byte)

            if child is None:
                return None

            if isinstance(child, Leaf):
                if child.key == key:
                    return child.value
                return None

            current = child
            depth += 1

    def _create_leaf(self, key: bytes, value: Value) -> Leaf:
        """Create a new leaf node."""
        return Leaf(key, value)

    def _grow_node(self, node: Node) -> Node:
        """Grow node to next size class."""
        if isinstance(node, Node4):
            new_node = Node16()
            new_node.header.prefix_len = node.header.prefix_len
            new_node.header.prefix = node.header.prefix[:]
            new_node.header.leaf_value = node.header.leaf_value

            for i in range(node.num_children):
                new_node.add_child(node.keys[i], node.children[i])

            return new_node

        elif isinstance(node, Node16):
            new_node = Node48()
            new_node.header.prefix_len = node.header.prefix_len
            new_node.header.prefix = node.header.prefix[:]
            new_node.header.leaf_value = node.header.leaf_value

            for i in range(node.num_children):
                new_node.add_child(node.keys[i], node.children[i])

            return new_node

        elif isinstance(node, Node48):
            new_node = Node256()
            new_node.header.prefix_len = node.header.prefix_len
            new_node.header.prefix = node.header.prefix[:]
            new_node.header.leaf_value = node.header.leaf_value

            for i in range(256):
                child = node.find_child(i)
                if child is not None:
                    new_node.add_child(i, child)

            return new_node

        else:
            raise RuntimeError("Cannot grow Node256")

    def insert(self, key: bytes, value: Value) -> None:
        """Insert or update key-value pair.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to insert
            value: Value to store
        """
        if self.root is None:
            # First key - just store as a leaf at root
            self.root = self._create_leaf(key, value)
            self._size = 1
            return

        if isinstance(self.root, Leaf):
            # Root is a leaf - check if it's the same key
            if self.root.key == key:
                self.root.value = value
                return
            # Different key - need to split into a node
            old_leaf = self.root
            new_leaf = self._create_leaf(key, value)
            self.root = self._create_parent_node(old_leaf, new_leaf)
            self._size += 1
            return

        # Root is a node - insert recursively
        self._insert_recursive(self.root, key, value, 0)

    def _create_parent_node(self, leaf1: Leaf, leaf2: Leaf) -> Node:
        """Create a parent node for two leaves.

        Finds the common prefix and creates a Node4 with both leaves as children.
        """
        key1, key2 = leaf1.key, leaf2.key

        # Find common prefix length
        common_len = 0
        min_len = min(len(key1), len(key2))
        while common_len < min_len and key1[common_len] == key2[common_len]:
            common_len += 1

        # Create new Node4
        node = Node4()
        node.header.prefix_len = common_len

        # Store common prefix (up to MAX_PREFIX_LEN)
        for i in range(min(common_len, MAX_PREFIX_LEN)):
            node.header.prefix[i] = key1[i]

        # Add first leaf
        if common_len < len(key1):
            byte1 = key1[common_len]
            node.add_child(byte1, leaf1)
        else:
            node.header.leaf_value = leaf1

        # Add second leaf
        if common_len < len(key2):
            byte2 = key2[common_len]
            node.add_child(byte2, leaf2)
        else:
            node.header.leaf_value = leaf2

        return node

    def _insert_recursive(
        self, node: Node, key: bytes, value: Value, depth: int
    ) -> bool:
        """Recursively insert key into tree.

        Returns:
            True if this was a new key (size increased)
        """
        # Check for prefix mismatch
        header = node.header
        prefix_len = header.prefix_len

        # Find mismatch position
        mismatch_idx = 0
        max_cmp = min(prefix_len, MAX_PREFIX_LEN)

        while (
            mismatch_idx < max_cmp
            and depth < len(key)
            and header.prefix[mismatch_idx] == key[depth]
        ):
            mismatch_idx += 1
            depth += 1

        # Check hidden prefix if needed
        if mismatch_idx == MAX_PREFIX_LEN and prefix_len > MAX_PREFIX_LEN:
            leaf = self._find_any_leaf(node)
            limit = min(len(leaf.key), len(key))
            while (
                mismatch_idx < prefix_len
                and depth < limit
                and leaf.key[depth] == key[depth]
            ):
                mismatch_idx += 1
                depth += 1

        # If there's a mismatch, we need to split
        if mismatch_idx < prefix_len:
            return self._split_node(node, key, value, depth, mismatch_idx)

        # No mismatch, check if we're at the end of the key
        if depth == len(key):
            # Update or create leaf value at this node
            if header.leaf_value is not None:
                header.leaf_value.value = value
                return False  # Updated existing
            else:
                header.leaf_value = self._create_leaf(key, value)
                self._size += 1
                return True

        # Follow or create child
        key_byte = key[depth]
        child = node.find_child(key_byte)

        if child is None:
            # Create new leaf as child
            new_leaf = self._create_leaf(key, value)

            # Try to add, grow if needed
            try:
                node.add_child(key_byte, new_leaf)
            except Exception:
                # Node is full, need to grow
                parent = self._get_parent_node(node)
                if parent is not None:
                    # This is complex - for now, just grow in place
                    pass

                grown = self._grow_node(node)
                self._replace_node(node, grown)
                grown.add_child(key_byte, new_leaf)

            self._size += 1
            return True

        if isinstance(child, Leaf):
            if child.key == key:
                # Update existing
                child.value = value
                return False

            # Split leaf into node
            return self._split_leaf(node, key_byte, child, key, value, depth + 1)

        # Recurse into child node
        return self._insert_recursive(child, key, value, depth + 1)

    def _split_node(
        self, node: Node, key: bytes, value: Value, depth: int, mismatch_idx: int
    ) -> bool:
        """Split a node due to prefix mismatch."""
        header = node.header

        # Create new Node4 to replace the mismatched part
        new_n4 = Node4()
        new_n4.header.prefix_len = mismatch_idx

        # Copy prefix up to mismatch
        for i in range(min(mismatch_idx, MAX_PREFIX_LEN)):
            new_n4.header.prefix[i] = header.prefix[i]

        # Adjust old node's prefix
        header.prefix_len -= mismatch_idx + 1

        if header.prefix_len > 0:
            new_prefix_len = min(header.prefix_len, MAX_PREFIX_LEN)
            leaf = self._find_any_leaf(node)
            for i in range(new_prefix_len):
                header.prefix[i] = leaf.key[depth + 1 + i]

        # Get the byte at mismatch position from existing node
        leaf = self._find_any_leaf(node)
        old_byte = leaf.key[depth]

        # Add old node to new_n4
        new_n4.add_child(old_byte, node)

        # Add new leaf to new_n4
        new_leaf = self._create_leaf(key, value)
        if depth == len(key):
            new_n4.header.leaf_value = new_leaf
        else:
            new_n4.add_child(key[depth], new_leaf)

        # Replace in tree
        self._replace_node(node, new_n4)
        self._size += 1
        return True

    def _split_leaf(
        self,
        parent: Node,
        key_byte: int,
        old_leaf: Leaf,
        new_key: bytes,
        new_value: Value,
        depth: int,
    ) -> bool:
        """Split a leaf into a new node."""
        # Create new Node4
        new_n4 = Node4()

        # Find common prefix between keys
        i = depth
        min_len = min(len(old_leaf.key), len(new_key))
        while i < min_len and old_leaf.key[i] == new_key[i]:
            i += 1

        prefix_len = i - depth
        new_n4.header.prefix_len = prefix_len

        for j in range(min(prefix_len, MAX_PREFIX_LEN)):
            new_n4.header.prefix[j] = new_key[depth + j]

        # Add old leaf
        if i < len(old_leaf.key):
            old_byte = old_leaf.key[i]
            new_n4.add_child(old_byte, old_leaf)
        else:
            new_n4.header.leaf_value = old_leaf

        # Add new leaf
        new_leaf = self._create_leaf(new_key, new_value)
        if i < len(new_key):
            new_byte = new_key[i]
            new_n4.add_child(new_byte, new_leaf)
        else:
            new_n4.header.leaf_value = new_leaf

        # Replace in parent
        parent.remove_child(key_byte)
        parent.add_child(key_byte, new_n4)

        self._size += 1
        return True

    def _get_parent_node(self, target: Node) -> Optional[Node]:
        """Find parent of a node (for growing)."""
        # This is expensive - in practice, we should track parent during traversal
        # For now, return None and handle at root level
        if self.root is target:
            return None
        return None  # Simplified - would need parent tracking

    def _replace_node(self, old: Node, new: Node) -> None:
        """Replace a node in the tree."""
        if self.root is old:
            self.root = new
            return

        # Would need parent tracking for full implementation
        # For now, this only works for root replacement
        raise RuntimeError("Can only replace root node in current implementation")

    def delete(self, key: bytes) -> bool:
        """Delete key from tree.

        Time Complexity: O(k) where k is key length

        Args:
            key: Key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        if self.root is None:
            return False

        # Handle root being a leaf
        if isinstance(self.root, Leaf):
            if self.root.key == key:
                self.root = None
                self._size -= 1
                return True
            return False

        deleted = self._delete_recursive(self.root, key, 0, None, None)
        if deleted:
            self._size -= 1
        return deleted

    def _delete_recursive(
        self,
        node: Node,
        key: bytes,
        depth: int,
        parent: Optional[Node],
        parent_byte: Optional[int],
    ) -> bool:
        """Recursively delete key from tree."""
        # Check prefix
        new_depth = self._match_prefix(node, key, depth)
        if new_depth is None:
            return False
        depth = new_depth

        # Check if this is the target node
        if depth == len(key):
            if node.header.leaf_value is not None:
                if node.header.leaf_value.key == key:
                    node.header.leaf_value = None

                    # If node is now empty, remove it
                    if node.num_children == 0 and parent is not None:
                        parent.remove_child(parent_byte)

                    return True
            return False

        # Follow child
        key_byte = key[depth]
        child = node.find_child(key_byte)

        if child is None:
            return False

        if isinstance(child, Leaf):
            if child.key == key:
                node.remove_child(key_byte)
                return True
            return False

        # Recurse
        return self._delete_recursive(child, key, depth + 1, node, key_byte)

    def scan_prefix(self, prefix: bytes) -> List[Tuple[bytes, Value]]:
        """Scan all keys with given prefix.

        Time Complexity: O(p + n) where p is prefix length, n is number of matches

        Args:
            prefix: Prefix to match

        Returns:
            List of (key, value) tuples sorted by key
        """
        if self.root is None:
            return []

        results: List[Tuple[bytes, Value]] = []

        # Find the node where prefix ends
        node, depth = self._find_prefix_node(self.root, prefix, 0)
        if node is None:
            return results

        # Collect all keys under this node
        self._collect_all(node, results)

        # Filter by prefix and sort
        results = [(k, v) for k, v in results if k.startswith(prefix)]
        results.sort(key=lambda x: x[0])

        return results

    def _find_prefix_node(
        self, node: Node, prefix: bytes, depth: int
    ) -> Tuple[Optional[Node], int]:
        """Find the node where the prefix ends."""
        while True:
            # Check if we've consumed the entire prefix
            if depth >= len(prefix):
                return node, depth

            # Check prefix bytes in node header
            header = node.header
            prefix_len = header.prefix_len
            max_cmp = min(prefix_len, MAX_PREFIX_LEN)

            for i in range(max_cmp):
                if depth >= len(prefix):
                    return node, depth
                if header.prefix[i] != prefix[depth]:
                    return None, depth
                depth += 1

            if prefix_len > MAX_PREFIX_LEN:
                leaf = self._find_any_leaf(node)
                for i in range(MAX_PREFIX_LEN, prefix_len):
                    if depth >= len(prefix):
                        return node, depth
                    if leaf.key[depth] != prefix[depth]:
                        return None, depth
                    depth += 1

            if depth == len(prefix):
                return node, depth

            # Follow child
            key_byte = prefix[depth]
            child = node.find_child(key_byte)

            if child is None:
                return None, depth

            if isinstance(child, Leaf):
                if child.key.startswith(prefix):
                    return node, depth
                return None, depth

            node = child
            depth += 1

    def _collect_all(self, node: Node, results: List[Tuple[bytes, Value]]) -> None:
        """Collect all keys under a node."""
        if node.header.leaf_value is not None:
            leaf = node.header.leaf_value
            results.append((leaf.key, leaf.value))

        # Iterate over children
        if isinstance(node, Node4):
            for i in range(node.num_children):
                child = node.children[i]
                if isinstance(child, Leaf):
                    results.append((child.key, child.value))
                else:
                    self._collect_all(child, results)

        elif isinstance(node, Node16):
            for i in range(node.num_children):
                child = node.children[i]
                if isinstance(child, Leaf):
                    results.append((child.key, child.value))
                else:
                    self._collect_all(child, results)

        elif isinstance(node, Node48):
            for i in range(256):
                child = node.find_child(i)
                if child is not None:
                    if isinstance(child, Leaf):
                        results.append((child.key, child.value))
                    else:
                        self._collect_all(child, results)

        elif isinstance(node, Node256):
            for i in range(256):
                child = node.find_child(i)
                if child is not None:
                    if isinstance(child, Leaf):
                        results.append((child.key, child.value))
                    else:
                        self._collect_all(child, results)

    def scan_range(self, start: bytes, end: bytes) -> List[Tuple[bytes, Value]]:
        """Scan keys in range [start, end).

        Time Complexity: O(k + n) where k is key length, n is number of matches

        Args:
            start: Start key (inclusive)
            end: End key (exclusive)

        Returns:
            List of (key, value) tuples sorted by key
        """
        if self.root is None:
            return []

        results: List[Tuple[bytes, Value]] = []
        self._scan_range_recursive(self.root, results, start, end)

        results.sort(key=lambda x: x[0])
        return results

    def _scan_range_recursive(
        self, node: Node, results: List[Tuple[bytes, Value]], start: bytes, end: bytes
    ) -> None:
        """Recursively scan range."""
        # Check leaf value at this node
        if node.header.leaf_value is not None:
            leaf = node.header.leaf_value
            if start <= leaf.key < end:
                results.append((leaf.key, leaf.value))

        # Iterate children in order
        children = self._get_sorted_children(node)

        for key_byte, child in children:
            if isinstance(child, Leaf):
                if start <= child.key < end:
                    results.append((child.key, child.value))
            else:
                self._scan_range_recursive(child, results, start, end)

    def _get_sorted_children(self, node: Node) -> List[Tuple[int, Union[Node, Leaf]]]:
        """Get children sorted by key byte."""
        children: List[Tuple[int, Union[Node, Leaf]]] = []

        if isinstance(node, Node4):
            for i in range(node.num_children):
                children.append((node.keys[i], node.children[i]))

        elif isinstance(node, Node16):
            for i in range(node.num_children):
                children.append((node.keys[i], node.children[i]))

        elif isinstance(node, Node48):
            for i in range(256):
                child = node.find_child(i)
                if child is not None:
                    children.append((i, child))

        elif isinstance(node, Node256):
            for i in range(256):
                child = node.find_child(i)
                if child is not None:
                    children.append((i, child))

        return children
