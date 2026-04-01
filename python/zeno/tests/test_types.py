"""Tests for zeno value types."""

from __future__ import annotations

import pytest

from zeno.types import Value


class TestValueClone:
    """Test value cloning behavior."""

    def test_clone_null(self):
        """Clone null value."""
        original = Value.null()
        cloned = original.clone()
        assert cloned.is_null()

    def test_clone_boolean(self):
        """Clone boolean value."""
        original = Value.boolean(True)
        cloned = original.clone()
        assert cloned.as_boolean() is True

        original2 = Value.boolean(False)
        cloned2 = original2.clone()
        assert cloned2.as_boolean() is False

    def test_clone_integer(self):
        """Clone integer value."""
        original = Value.integer(42)
        cloned = original.clone()
        assert cloned.as_integer() == 42

    def test_clone_float(self):
        """Clone float value."""
        original = Value.float(3.14)
        cloned = original.clone()
        assert cloned.as_float() == 3.14

    def test_clone_string(self):
        """Clone string value creates independent copy."""
        original = Value.string("hello")
        cloned = original.clone()
        assert cloned.as_string() == "hello"
        # Strings are immutable in Python, so identity may be the same
        # but value is definitely copied

    def test_clone_bytes(self):
        """Clone bytes value creates independent copy."""
        data = b"binary data"
        original = Value.bytes(data)
        cloned = original.clone()
        assert cloned.as_bytes() == data
        # Bytes are immutable in Python, so identity may be the same
        # but value is definitely copied

    def test_clone_array(self):
        """Clone array with nested values."""
        original = Value.array(
            [
                Value.string("alpha"),
                Value.integer(7),
                Value.boolean(True),
            ]
        )
        cloned = original.clone()

        arr = cloned.as_array()
        assert len(arr) == 3
        assert arr[0].as_string() == "alpha"
        assert arr[1].as_integer() == 7
        assert arr[2].as_boolean() is True

    def test_clone_object(self):
        """Clone object with nested values."""
        original = Value.object(
            {
                "message": Value.string("hello"),
                "count": Value.integer(42),
                "flag": Value.boolean(True),
            }
        )
        cloned = original.clone()

        obj = cloned.as_object()
        assert obj["message"].as_string() == "hello"
        assert obj["count"].as_integer() == 42
        assert obj["flag"].as_boolean() is True

    def test_clone_array_independence(self):
        """Cloned array elements are independent from original."""
        original = Value.array(
            [
                Value.string("alpha"),
                Value.integer(7),
            ]
        )
        cloned = original.clone()

        # Modify original
        original._data[0] = Value.string("omega")

        # Cloned should be unchanged
        assert cloned.as_array()[0].as_string() == "alpha"
        assert cloned.as_array()[1].as_integer() == 7

    def test_clone_object_independence(self):
        """Cloned object entries are independent from original."""
        original = Value.object(
            {
                "message": Value.string("hello"),
            }
        )
        cloned = original.clone()

        # Modify original
        original._data["message"] = Value.string("jello")

        # Cloned should be unchanged
        assert cloned.as_object()["message"].as_string() == "hello"

    def test_clone_deeply_nested(self):
        """Clone deeply nested structure."""
        original = Value.object(
            {
                "user": Value.object(
                    {
                        "name": Value.string("Alice"),
                        "scores": Value.array(
                            [
                                Value.integer(100),
                                Value.integer(95),
                            ]
                        ),
                    }
                ),
            }
        )
        cloned = original.clone()

        user = cloned.as_object()["user"].as_object()
        assert user["name"].as_string() == "Alice"
        assert user["scores"].as_array()[0].as_integer() == 100


class TestValueEquality:
    """Test value equality comparison."""

    def test_equal_scalars(self):
        """Scalar values are equal if same type and value."""
        assert Value.integer(42) == Value.integer(42)
        assert Value.boolean(True) == Value.boolean(True)
        assert Value.float(3.14) == Value.float(3.14)
        assert Value.string("hello") == Value.string("hello")
        assert Value.null() == Value.null()

    def test_not_equal_different_types(self):
        """Values of different types are not equal."""
        assert Value.integer(42) != Value.string("42")
        assert Value.boolean(True) != Value.integer(1)
        assert Value.null() != Value.boolean(False)

    def test_equal_arrays(self):
        """Arrays are equal if elements are equal."""
        arr1 = Value.array([Value.integer(1), Value.string("two")])
        arr2 = Value.array([Value.integer(1), Value.string("two")])
        assert arr1 == arr2

    def test_not_equal_arrays(self):
        """Arrays with different elements are not equal."""
        arr1 = Value.array([Value.integer(1), Value.integer(2)])
        arr2 = Value.array([Value.integer(1), Value.integer(3)])
        assert arr1 != arr2

    def test_equal_objects(self):
        """Objects are equal if key-value pairs are equal."""
        obj1 = Value.object({"a": Value.integer(1), "b": Value.string("x")})
        obj2 = Value.object({"b": Value.string("x"), "a": Value.integer(1)})
        assert obj1 == obj2

    def test_not_equal_objects(self):
        """Objects with different key-value pairs are not equal."""
        obj1 = Value.object({"a": Value.integer(1)})
        obj2 = Value.object({"a": Value.integer(2)})
        assert obj1 != obj2


class TestValueFromPython:
    """Test creating Value from Python objects."""

    def test_from_none(self):
        """Create from None."""
        v = Value.from_python(None)
        assert v.is_null()

    def test_from_bool(self):
        """Create from bool."""
        assert Value.from_python(True).as_boolean() is True
        assert Value.from_python(False).as_boolean() is False

    def test_from_int(self):
        """Create from int."""
        assert Value.from_python(42).as_integer() == 42
        assert Value.from_python(-100).as_integer() == -100

    def test_from_float(self):
        """Create from float."""
        assert Value.from_python(3.14).as_float() == 3.14

    def test_from_str(self):
        """Create from str."""
        assert Value.from_python("hello").as_string() == "hello"

    def test_from_bytes(self):
        """Create from bytes."""
        data = b"binary"
        assert Value.from_python(data).as_bytes() == data

    def test_from_list(self):
        """Create from list."""
        v = Value.from_python([1, "two", True, None])
        arr = v.as_array()
        assert arr[0].as_integer() == 1
        assert arr[1].as_string() == "two"
        assert arr[2].as_boolean() is True
        assert arr[3].is_null()

    def test_from_dict(self):
        """Create from dict."""
        v = Value.from_python({"name": "Alice", "age": 30})
        obj = v.as_object()
        assert obj["name"].as_string() == "Alice"
        assert obj["age"].as_integer() == 30

    def test_from_nested(self):
        """Create from nested structure."""
        data = {
            "users": [
                {"name": "Alice", "tags": ["admin", "user"]},
                {"name": "Bob", "tags": ["user"]},
            ]
        }
        v = Value.from_python(data)
        users = v.as_object()["users"].as_array()
        assert len(users) == 2
        assert users[0].as_object()["name"].as_string() == "Alice"


class TestValueToPython:
    """Test converting Value to Python objects."""

    def test_to_python_scalars(self):
        """Convert scalars back to Python."""
        assert Value.null().to_python() is None
        assert Value.boolean(True).to_python() is True
        assert Value.integer(42).to_python() == 42
        assert Value.float(3.14).to_python() == 3.14
        assert Value.string("hello").to_python() == "hello"
        assert Value.bytes(b"data").to_python() == b"data"

    def test_to_python_array(self):
        """Convert array to Python list."""
        v = Value.array([Value.integer(1), Value.string("two")])
        result = v.to_python()
        assert result == [1, "two"]

    def test_to_python_object(self):
        """Convert object to Python dict."""
        v = Value.object({"a": Value.integer(1), "b": Value.string("x")})
        result = v.to_python()
        assert result == {"a": 1, "b": "x"}

    def test_to_python_nested(self):
        """Convert nested structure."""
        v = Value.object(
            {
                "data": Value.array(
                    [
                        Value.object({"id": Value.integer(1)}),
                    ]
                ),
            }
        )
        result = v.to_python()
        assert result == {"data": [{"id": 1}]}


class TestValueTypeChecks:
    """Test type checking methods."""

    def test_is_methods(self):
        """Test all is_* methods."""
        assert Value.null().is_null()
        assert Value.boolean(True).is_boolean()
        assert Value.integer(1).is_integer()
        assert Value.float(1.0).is_float()
        assert Value.string("x").is_string()
        assert Value.bytes(b"x").is_bytes()
        assert Value.array([]).is_array()
        assert Value.object({}).is_object()

    def test_is_mutually_exclusive(self):
        """Type checks are mutually exclusive."""
        v = Value.integer(42)
        assert v.is_integer()
        assert not v.is_null()
        assert not v.is_boolean()
        assert not v.is_float()
        assert not v.is_string()
        assert not v.is_bytes()
        assert not v.is_array()
        assert not v.is_object()


class TestValueAsMethods:
    """Test accessor methods."""

    def test_as_boolean(self):
        """Access boolean value."""
        assert Value.boolean(True).as_boolean() is True

    def test_as_integer(self):
        """Access integer value."""
        assert Value.integer(42).as_integer() == 42

    def test_as_float(self):
        """Access float value."""
        assert Value.float(3.14).as_float() == 3.14

    def test_as_string(self):
        """Access string value."""
        assert Value.string("hello").as_string() == "hello"

    def test_as_bytes(self):
        """Access bytes value."""
        assert Value.bytes(b"data").as_bytes() == b"data"

    def test_as_array(self):
        """Access array value."""
        arr = [Value.integer(1)]
        assert Value.array(arr).as_array() == arr

    def test_as_object(self):
        """Access object value."""
        obj = {"key": Value.string("value")}
        assert Value.object(obj).as_object() == obj

    def test_as_wrong_type_raises(self):
        """Accessing wrong type raises TypeError."""
        with pytest.raises(TypeError):
            Value.integer(42).as_string()

        with pytest.raises(TypeError):
            Value.string("x").as_integer()
