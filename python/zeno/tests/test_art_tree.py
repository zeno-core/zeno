"""Tests for ART Tree operations."""

from __future__ import annotations

import pytest

from zeno.art.tree import Tree
from zeno.types import Value


class TestTreeBasic:
    """Test basic tree operations."""

    def test_init(self):
        """Tree initializes empty."""
        tree = Tree()
        assert tree.is_empty()

    def test_insert_and_lookup_single(self):
        """Insert and lookup single key."""
        tree = Tree()
        tree.insert(b"key", Value.string("value"))

        result = tree.lookup(b"key")
        assert result is not None
        assert result.as_string() == "value"

    def test_lookup_nonexistent(self):
        """Lookup non-existent key returns None."""
        tree = Tree()
        assert tree.lookup(b"missing") is None

    def test_insert_overwrite(self):
        """Insert overwrites existing key."""
        tree = Tree()
        tree.insert(b"key", Value.string("first"))
        tree.insert(b"key", Value.string("second"))

        result = tree.lookup(b"key")
        assert result.as_string() == "second"

    def test_delete_existing(self):
        """Delete existing key."""
        tree = Tree()
        tree.insert(b"key", Value.string("value"))

        deleted = tree.delete(b"key")
        assert deleted is True
        assert tree.lookup(b"key") is None

    def test_delete_nonexistent(self):
        """Delete non-existent key returns False."""
        tree = Tree()
        deleted = tree.delete(b"missing")
        assert deleted is False


class TestTreeMultipleKeys:
    """Test tree with multiple keys."""

    def test_insert_multiple_keys(self):
        """Insert and retrieve multiple keys."""
        tree = Tree()
        keys_values = [
            (b"aaa", Value.integer(1)),
            (b"aab", Value.integer(2)),
            (b"aba", Value.integer(3)),
            (b"abb", Value.integer(4)),
        ]

        for key, value in keys_values:
            tree.insert(key, value)

        for key, value in keys_values:
            result = tree.lookup(key)
            assert result == value

    def test_prefix_sharing(self):
        """Keys with shared prefix are stored efficiently."""
        tree = Tree()

        # These share prefix "user:"
        tree.insert(b"user:1", Value.string("alice"))
        tree.insert(b"user:2", Value.string("bob"))
        tree.insert(b"user:3", Value.string("charlie"))

        assert tree.lookup(b"user:1").as_string() == "alice"
        assert tree.lookup(b"user:2").as_string() == "bob"
        assert tree.lookup(b"user:3").as_string() == "charlie"


class TestTreeScan:
    """Test scan operations."""

    def test_scan_prefix_simple(self):
        """Scan keys with prefix."""
        tree = Tree()
        tree.insert(b"user:1", Value.string("alice"))
        tree.insert(b"user:2", Value.string("bob"))
        tree.insert(b"post:1", Value.string("post1"))
        tree.insert(b"user:3", Value.string("charlie"))

        results = tree.scan_prefix(b"user:")

        assert len(results) == 3
        keys = [r[0] for r in results]
        assert b"user:1" in keys
        assert b"user:2" in keys
        assert b"user:3" in keys

    def test_scan_prefix_empty(self):
        """Scan with non-matching prefix returns empty."""
        tree = Tree()
        tree.insert(b"aaa", Value.null())

        results = tree.scan_prefix(b"bbb")
        assert len(results) == 0

    def test_scan_prefix_all(self):
        """Scan with empty prefix returns all keys."""
        tree = Tree()
        tree.insert(b"aaa", Value.integer(1))
        tree.insert(b"bbb", Value.integer(2))

        results = tree.scan_prefix(b"")
        assert len(results) == 2

    def test_scan_results_sorted(self):
        """Scan results are sorted lexicographically."""
        tree = Tree()
        keys = [b"z", b"a", b"m", b"b"]
        for key in keys:
            tree.insert(key, Value.null())

        results = tree.scan_prefix(b"")
        result_keys = [r[0] for r in results]

        assert result_keys == sorted(keys)


class TestTreeRange:
    """Test range scan operations."""

    def test_scan_range(self):
        """Scan keys in range [start, end)."""
        tree = Tree()
        for i in range(10):
            tree.insert(bytes([97 + i]), Value.integer(i))  # a, b, c, ...

        results = tree.scan_range(b"c", b"g")  # c, d, e, f

        keys = [r[0] for r in results]
        assert keys == [b"c", b"d", b"e", b"f"]

    def test_scan_range_empty_result(self):
        """Range scan with no matches returns empty."""
        tree = Tree()
        tree.insert(b"aaa", Value.null())

        results = tree.scan_range(b"bbb", b"ccc")
        assert len(results) == 0

    def test_scan_range_single_key(self):
        """Range scan can return single key."""
        tree = Tree()
        tree.insert(b"bbb", Value.integer(1))

        results = tree.scan_range(b"bbb", b"ccc")
        assert len(results) == 1
        assert results[0][0] == b"bbb"


class TestTreeEdgeCases:
    """Test edge cases."""

    def test_empty_key(self):
        """Tree can handle empty key."""
        tree = Tree()
        tree.insert(b"", Value.string("empty"))

        result = tree.lookup(b"")
        assert result.as_string() == "empty"

    def test_binary_keys(self):
        """Tree handles binary keys with all byte values."""
        tree = Tree()

        # Keys with null bytes and high bytes
        keys = [
            bytes([0, 1, 2]),
            bytes([255, 254, 253]),
            bytes([127, 128, 129]),
        ]

        for i, key in enumerate(keys):
            tree.insert(key, Value.integer(i))

        for i, key in enumerate(keys):
            assert tree.lookup(key).as_integer() == i

    def test_long_keys(self):
        """Tree handles long keys with path compression."""
        tree = Tree()
        key = b"a" * 1000

        tree.insert(key, Value.string("long"))
        result = tree.lookup(key)
        assert result.as_string() == "long"

    def test_delete_all_keys(self):
        """Delete all keys leaves empty tree."""
        tree = Tree()
        keys = [b"a", b"b", b"c"]

        for key in keys:
            tree.insert(key, Value.null())

        for key in keys:
            tree.delete(key)

        assert tree.is_empty()


class TestTreeNodeGrowth:
    """Test node growth transitions."""

    def test_node4_to_node16(self):
        """Node4 grows to Node16 when full."""
        tree = Tree()

        # Add 5 children (exceeds Node4 capacity)
        for i in range(5):
            tree.insert(bytes([i]), Value.integer(i))

        # All should be accessible
        for i in range(5):
            assert tree.lookup(bytes([i])).as_integer() == i

    def test_node16_to_node48(self):
        """Node16 grows to Node48 when full."""
        tree = Tree()

        # Add 17 children
        for i in range(17):
            tree.insert(bytes([i]), Value.integer(i))

        for i in range(17):
            assert tree.lookup(bytes([i])).as_integer() == i

    def test_node48_to_node256(self):
        """Node48 grows to Node256 when full."""
        tree = Tree()

        # Add 49 children
        for i in range(49):
            tree.insert(bytes([i]), Value.integer(i))

        for i in range(49):
            assert tree.lookup(bytes([i])).as_integer() == i

    def test_node_growth_with_divergent_keys(self):
        """Node growth with keys that diverge at different positions."""
        tree = Tree()

        # Keys share prefix but diverge
        keys = [
            b"aaa",
            b"aab",
            b"aac",
            b"aad",
            b"aba",
            b"abb",
            b"abc",
            b"abd",
        ]

        for i, key in enumerate(keys):
            tree.insert(key, Value.integer(i))

        for i, key in enumerate(keys):
            assert tree.lookup(key).as_integer() == i
