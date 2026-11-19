"""Process-local references to Snowflake plans inside a Polars expression."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4
from weakref import WeakValueDictionary

import polars as pl

if TYPE_CHECKING:
    from ._core import _Frame
    from ._frame import DataFrame

_PREFIX = "snowpark_rs:membership:"
_REFERENCES: WeakValueDictionary[str, _QueryReference] = WeakValueDictionary()


def _fail_local(_values: pl.Series) -> pl.Series:
    raise pl.exceptions.InvalidOperationError(
        "a Snowflake membership subquery requires a snowpark_rs.Table"
    )


def _restore_callback():
    return _fail_local


class _QueryReference:
    def __init__(self, source: DataFrame) -> None:
        self.source = source

    def __call__(self, values: pl.Series) -> pl.Series:
        return _fail_local(values)

    def __reduce__(self):
        # Polars keeps this callback (and the plan) alive in the expression.
        # Serialization exposes only the marker, so the compiler can read the
        # token from the input literal without deserializing a Python callable.
        return _restore_callback, ()


def reference(source: DataFrame) -> pl.Expr:
    token = _PREFIX + uuid4().hex
    handle = _QueryReference(source)
    _REFERENCES[token] = handle
    return pl.lit(token).map_batches(handle, return_dtype=pl.Null)


def referenced_frame(token: str) -> _Frame | None:
    handle = _REFERENCES.get(token)
    return handle.source._native if handle is not None else None
