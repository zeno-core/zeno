"""Value types for zeno KV store.

This module provides a JSON-like value system that supports:
- null
- bool
- int (i64 range)
- float (f64)
- str
- bytes
- list/array
- dict/object

All operations are designed to be safe and follow the semantics
of Zig's Value type from the original implementation.
"""

from __future__ import annotations

import copy
from enum import Enum, auto
from typing import Any, Dict, List, Union


class ValueType(Enum):
    """Value type enumeration."""

    NULL = auto()
    BOOLEAN = auto()
    INTEGER = auto()
    FLOAT = auto()
    STRING = auto()
    BYTES = auto()
    ARRAY = auto()
    OBJECT = auto()


class Value:
    """A JSON-like value type supporting nested structures.

    This is the Python port of Zig's Value union, designed to:
    - Support deep cloning for safe storage
    - Provide type-safe access
    - Enable conversion to/from Python native types
    """

    __slots__ = ("_type", "_data")

    def __init__(self, value_type: ValueType, data: Any) -> None:
        """Initialize a value. Use factory methods instead."""
        self._type = value_type
        self._data = data

    # Factory methods
    @classmethod
    def null(cls) -> Value:
        """Create a null value."""
        return cls(ValueType.NULL, None)

    @classmethod
    def boolean(cls, value: bool) -> Value:
        """Create a boolean value."""
        return cls(ValueType.BOOLEAN, bool(value))

    @classmethod
    def integer(cls, value: int) -> Value:
        """Create an integer value."""
        return cls(ValueType.INTEGER, int(value))

    @classmethod
    def float(cls, value: float) -> Value:
        """Create a float value."""
        return cls(ValueType.FLOAT, float(value))

    @classmethod
    def string(cls, value: str) -> Value:
        """Create a string value."""
        return cls(ValueType.STRING, str(value))

    @classmethod
    def bytes(cls, value: bytes) -> Value:
        """Create a bytes value."""
        return cls(ValueType.BYTES, bytes(value))

    @classmethod
    def array(cls, items: List[Value]) -> Value:
        """Create an array value from a list of Values."""
        return cls(ValueType.ARRAY, list(items))

    @classmethod
    def object(cls, entries: Dict[str, Value]) -> Value:
        """Create an object value from a dict of Values."""
        return cls(ValueType.OBJECT, dict(entries))

    # Type checking methods
    def is_null(self) -> bool:
        """Check if value is null."""
        return self._type == ValueType.NULL

    def is_boolean(self) -> bool:
        """Check if value is boolean."""
        return self._type == ValueType.BOOLEAN

    def is_integer(self) -> bool:
        """Check if value is integer."""
        return self._type == ValueType.INTEGER

    def is_float(self) -> bool:
        """Check if value is float."""
        return self._type == ValueType.FLOAT

    def is_string(self) -> bool:
        """Check if value is string."""
        return self._type == ValueType.STRING

    def is_bytes(self) -> bool:
        """Check if value is bytes."""
        return self._type == ValueType.BYTES

    def is_array(self) -> bool:
        """Check if value is array."""
        return self._type == ValueType.ARRAY

    def is_object(self) -> bool:
        """Check if value is object."""
        return self._type == ValueType.OBJECT

    # Accessor methods
    def as_boolean(self) -> bool:
        """Get value as boolean. Raises TypeError if wrong type."""
        if not self.is_boolean():
            raise TypeError(f"Expected boolean, got {self._type.name}")
        return self._data

    def as_integer(self) -> int:
        """Get value as integer. Raises TypeError if wrong type."""
        if not self.is_integer():
            raise TypeError(f"Expected integer, got {self._type.name}")
        return self._data

    def as_float(self) -> float:
        """Get value as float. Raises TypeError if wrong type."""
        if not self.is_float():
            raise TypeError(f"Expected float, got {self._type.name}")
        return self._data

    def as_string(self) -> str:
        """Get value as string. Raises TypeError if wrong type."""
        if not self.is_string():
            raise TypeError(f"Expected string, got {self._type.name}")
        return self._data

    def as_bytes(self) -> bytes:
        """Get value as bytes. Raises TypeError if wrong type."""
        if not self.is_bytes():
            raise TypeError(f"Expected bytes, got {self._type.name}")
        return self._data

    def as_array(self) -> List[Value]:
        """Get value as array. Raises TypeError if wrong type."""
        if not self.is_array():
            raise TypeError(f"Expected array, got {self._type.name}")
        return self._data

    def as_object(self) -> Dict[str, Value]:
        """Get value as object. Raises TypeError if wrong type."""
        if not self.is_object():
            raise TypeError(f"Expected object, got {self._type.name}")
        return self._data

    # Deep clone - mimics Zig's clone behavior
    def clone(self) -> Value:
        """Deep clone the value and all nested children.

        This creates independent copies of all mutable data:
        - strings are duplicated
        - bytes are duplicated
        - arrays are duplicated with cloned elements
        - objects are duplicated with cloned values

        Returns a new Value that can be modified independently.
        """
        if self._type == ValueType.NULL:
            return Value.null()
        elif self._type == ValueType.BOOLEAN:
            return Value.boolean(self._data)
        elif self._type == ValueType.INTEGER:
            return Value.integer(self._data)
        elif self._type == ValueType.FLOAT:
            return Value.float(self._data)
        elif self._type == ValueType.STRING:
            return Value.string(self._data)
        elif self._type == ValueType.BYTES:
            return Value.bytes(self._data)
        elif self._type == ValueType.ARRAY:
            return Value.array([item.clone() for item in self._data])
        elif self._type == ValueType.OBJECT:
            return Value.object({k: v.clone() for k, v in self._data.items()})
        else:
            raise RuntimeError(f"Unknown value type: {self._type}")

    # Conversion from Python native types
    @classmethod
    def from_python(cls, obj: Any) -> Value:
        """Convert a Python object to a Value.

        Supports:
        - None -> null
        - bool -> boolean
        - int -> integer
        - float -> float
        - str -> string
        - bytes -> bytes
        - list -> array (recursively converts elements)
        - dict -> object (recursively converts values, keys must be strings)

        Raises:
            TypeError: If the type is not supported or dict has non-string keys.
        """
        if obj is None:
            return cls.null()
        elif isinstance(obj, bool):
            return cls.boolean(obj)
        elif isinstance(obj, int):
            return cls.integer(obj)
        elif isinstance(obj, float):
            return cls.float(obj)
        elif isinstance(obj, str):
            return cls.string(obj)
        elif isinstance(obj, bytes):
            return cls.bytes(obj)
        elif isinstance(obj, list):
            return cls.array([cls.from_python(item) for item in obj])
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if not isinstance(k, str):
                    raise TypeError(
                        f"Object keys must be strings, got {type(k).__name__}"
                    )
                result[k] = cls.from_python(v)
            return cls.object(result)
        else:
            raise TypeError(f"Cannot convert {type(obj).__name__} to Value")

    # Conversion to Python native types
    def to_python(self) -> Any:
        """Convert this Value to a Python native type.

        - null -> None
        - boolean -> bool
        - integer -> int
        - float -> float
        - string -> str
        - bytes -> bytes
        - array -> list
        - object -> dict
        """
        if self._type == ValueType.NULL:
            return None
        elif self._type in (
            ValueType.BOOLEAN,
            ValueType.INTEGER,
            ValueType.FLOAT,
            ValueType.STRING,
            ValueType.BYTES,
        ):
            return self._data
        elif self._type == ValueType.ARRAY:
            return [item.to_python() for item in self._data]
        elif self._type == ValueType.OBJECT:
            return {k: v.to_python() for k, v in self._data.items()}
        else:
            raise RuntimeError(f"Unknown value type: {self._type}")

    # Equality comparison
    def __eq__(self, other: object) -> bool:
        """Check equality with another Value."""
        if not isinstance(other, Value):
            return NotImplemented
        if self._type != other._type:
            return False
        if self._type in (
            ValueType.NULL,
            ValueType.BOOLEAN,
            ValueType.INTEGER,
            ValueType.FLOAT,
            ValueType.STRING,
            ValueType.BYTES,
        ):
            return self._data == other._data
        elif self._type == ValueType.ARRAY:
            if len(self._data) != len(other._data):
                return False
            return all(a == b for a, b in zip(self._data, other._data))
        elif self._type == ValueType.OBJECT:
            if set(self._data.keys()) != set(other._data.keys()):
                return False
            return all(self._data[k] == other._data[k] for k in self._data)
        return False

    def __hash__(self) -> int:
        """Hash based on type and value."""
        if self._type in (ValueType.ARRAY, ValueType.OBJECT):
            raise TypeError("unhashable type: 'Value' (array or object)")
        return hash((self._type, self._data))

    def __repr__(self) -> str:
        """String representation for debugging."""
        if self._type == ValueType.NULL:
            return "Value.null()"
        elif self._type == ValueType.BOOLEAN:
            return f"Value.boolean({self._data!r})"
        elif self._type == ValueType.INTEGER:
            return f"Value.integer({self._data})"
        elif self._type == ValueType.FLOAT:
            return f"Value.float({self._data})"
        elif self._type == ValueType.STRING:
            return f"Value.string({self._data!r})"
        elif self._type == ValueType.BYTES:
            return f"Value.bytes({self._data!r})"
        elif self._type == ValueType.ARRAY:
            return f"Value.array({self._data!r})"
        elif self._type == ValueType.OBJECT:
            return f"Value.object({self._data!r})"
        else:
            return f"Value(<?>, {self._data!r})"
