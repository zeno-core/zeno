"""Exceptions for zeno KV store."""

from __future__ import annotations


class ZenoError(Exception):
    """Base exception for zeno errors."""

    pass


class KeyTooLarge(ZenoError):
    """Key exceeds maximum allowed length."""

    pass


class KeyNotFound(ZenoError):
    """Key does not exist in the database."""

    pass


class InvalidKey(ZenoError):
    """Key is invalid (e.g., empty)."""

    pass


class ARTError(ZenoError):
    """ART index error."""

    pass


class NodeFull(ARTError):
    """Node cannot accept more children."""

    pass


class NodeEmpty(ARTError):
    """Node has no children."""

    pass
