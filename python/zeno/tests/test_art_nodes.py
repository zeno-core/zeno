"""Tests for ART (Adaptive Radix Tree) nodes."""

from __future__ import annotations

import pytest

from zeno.art.node import Node4, Node16, Node48, Node256, NodeType
from zeno.art.leaf import Leaf
from zeno.types import Value


class TestNode4:
    """Test Node4 operations."""

    def test_init(self):
        """Node4 initializes correctly."""
        node = Node4()
        assert node.node_type == NodeType.NODE4
        assert node.num_children == 0
        assert node.prefix_len == 0
        assert node.leaf_value is None

    def test_find_child_empty(self):
        """Finding child in empty node returns None."""
        node = Node4()
        assert node.find_child(65) is None

    def test_add_child(self):
        """Add child to node."""
        node = Node4()
        leaf = Leaf(b"key", Value.string("value"))

        node.add_child(65, leaf)  # 'A'
        assert node.num_children == 1
        assert node.find_child(65) is leaf

    def test_add_multiple_children(self):
        """Add multiple children in sorted order."""
        node = Node4()
        leaf_a = Leaf(b"a", Value.string("a"))
        leaf_c = Leaf(b"c", Value.string("c"))
        leaf_b = Leaf(b"b", Value.string("b"))

        node.add_child(99, leaf_c)  # 'c'
        node.add_child(97, leaf_a)  # 'a'
        node.add_child(98, leaf_b)  # 'b'

        assert node.num_children == 3
        # Keys should be sorted: [97, 98, 99]
        assert node.keys[0] == 97
        assert node.keys[1] == 98
        assert node.keys[2] == 99

    def test_add_child_full(self):
        """Adding to full Node4 raises NodeFull."""
        node = Node4()
        for i in range(4):
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        with pytest.raises(Exception):  # NodeFull
            node.add_child(99, Leaf(b"full", Value.null()))

    def test_remove_child(self):
        """Remove child from node."""
        node = Node4()
        leaf = Leaf(b"key", Value.string("value"))
        node.add_child(65, leaf)

        node.remove_child(65)
        assert node.num_children == 0
        assert node.find_child(65) is None

    def test_find_child_not_found(self):
        """Find non-existent child returns None."""
        node = Node4()
        node.add_child(65, Leaf(b"a", Value.null()))
        assert node.find_child(66) is None


class TestNode16:
    """Test Node16 operations."""

    def test_init(self):
        """Node16 initializes correctly."""
        node = Node16()
        assert node.node_type == NodeType.NODE16
        assert node.num_children == 0

    def test_add_and_find_children(self):
        """Add and find multiple children."""
        node = Node16()
        leaves = {i: Leaf(bytes([i]), Value.integer(i)) for i in range(10)}

        for byte, leaf in leaves.items():
            node.add_child(byte, leaf)

        assert node.num_children == 10

        for byte, leaf in leaves.items():
            assert node.find_child(byte) is leaf

    def test_children_sorted(self):
        """Children are kept sorted by key byte."""
        node = Node16()

        # Add in reverse order
        for i in range(10, 0, -1):
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        # Check sorted order
        for i in range(9):
            assert node.keys[i] < node.keys[i + 1]

    def test_add_child_full(self):
        """Adding to full Node16 raises NodeFull."""
        node = Node16()
        for i in range(16):
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        with pytest.raises(Exception):
            node.add_child(99, Leaf(b"full", Value.null()))


class TestNode48:
    """Test Node48 operations."""

    def test_init(self):
        """Node48 initializes correctly."""
        node = Node48()
        assert node.node_type == NodeType.NODE48
        assert node.num_children == 0

    def test_add_and_find_children(self):
        """Add and find multiple children."""
        node = Node48()

        # Add children at sparse locations
        indices = [0, 50, 100, 150, 200, 255]
        for i in indices:
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        assert node.num_children == len(indices)

        for i in indices:
            assert node.find_child(i) is not None

    def test_find_child_not_found(self):
        """Find non-existent child returns None."""
        node = Node48()
        node.add_child(50, Leaf(b"x", Value.null()))
        assert node.find_child(51) is None

    def test_remove_child(self):
        """Remove child from node."""
        node = Node48()
        node.add_child(50, Leaf(b"x", Value.null()))

        node.remove_child(50)
        assert node.num_children == 0
        assert node.find_child(50) is None

    def test_add_child_full(self):
        """Adding to full Node48 raises NodeFull."""
        node = Node48()
        for i in range(48):
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        with pytest.raises(Exception):
            node.add_child(99, Leaf(b"full", Value.null()))


class TestNode256:
    """Test Node256 operations."""

    def test_init(self):
        """Node256 initializes correctly."""
        node = Node256()
        assert node.node_type == NodeType.NODE256
        assert node.num_children == 0

    def test_add_and_find_children(self):
        """Add and find children at any byte position."""
        node = Node256()

        # Add at arbitrary positions
        indices = [0, 127, 255]
        for i in indices:
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        assert node.num_children == len(indices)

        for i in indices:
            assert node.find_child(i) is not None

    def test_remove_child(self):
        """Remove child from node."""
        node = Node256()
        node.add_child(100, Leaf(b"x", Value.null()))

        node.remove_child(100)
        assert node.num_children == 0
        assert node.find_child(100) is None

    def test_can_hold_all_bytes(self):
        """Node256 can theoretically hold all 256 byte values."""
        node = Node256()

        # Add many children
        for i in range(0, 256, 2):  # Every other byte
            node.add_child(i, Leaf(bytes([i]), Value.integer(i)))

        assert node.num_children == 128


class TestLeaf:
    """Test Leaf node."""

    def test_init(self):
        """Leaf initializes correctly."""
        value = Value.string("hello")
        leaf = Leaf(b"key", value)

        assert leaf.key == b"key"
        assert leaf.value is value

    def test_value_accessor(self):
        """Value can be accessed and modified."""
        leaf = Leaf(b"key", Value.integer(42))
        assert leaf.value.as_integer() == 42

        leaf.value = Value.string("new")
        assert leaf.value.as_string() == "new"

    def test_full_key_storage(self):
        """Leaf stores full key for path compression."""
        key = b"very/long/key/path"
        leaf = Leaf(key, Value.null())
        assert leaf.key == key
