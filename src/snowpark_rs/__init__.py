from typing import TYPE_CHECKING

import polars as pl

from ._core import Field, UnsupportedPolarsExpression
from ._frame import DataFrame, Table
from ._snowflake import col, try_parse_json

if pl.__version__ != "2.1.0" or pl.get_index_type() != pl.UInt32:
    raise ImportError("snowpark-rs requires the 32-bit runtime of Polars 2.1.0")

if TYPE_CHECKING:
    from ._session import Session as Session

__all__ = [
    "DataFrame",
    "Field",
    "Table",
    "UnsupportedPolarsExpression",
    "col",
    "try_parse_json",
]


def __getattr__(name: str) -> object:
    if name == "Session":
        from ._session import Session, require_backend

        require_backend()
        return Session
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
