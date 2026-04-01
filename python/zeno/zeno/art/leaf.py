"""ART leaf node implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zeno.types import Value


class Leaf:
    """Leaf node in the ART.

    Stores the full key and a reference to the value.
    The full key is essential for path compression and splitting.
    """

    __slots__ = ("key", "_value")

    def __init__(self, key: bytes, value: "Value") -> None:
        """Initialize a leaf.

        Args:
            key: Full key (bytes)
            value: Associated value
        """
        self.key: bytes = key
        self._value: "Value" = value

    @property
    def value(self) -> "Value":
        """Get the value."""
        return self._value

    @value.setter
    def value(self, new_value: "Value") -> None:
        """Set a new value."""
        self._value = new_value

    def __repr__(self) -> str:
        return f"Leaf(key={self.key!r}, value={self._value!r})"
