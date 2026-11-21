"""Snowflake-specific functions that compose with Polars expressions."""

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol, cast

from polars.plugins import register_plugin_function

import polars as pl

from ._core import __file__ as _core_path

PLUGIN_PATH = Path(_core_path).resolve()
PLUGIN_SYMBOL = "try_parse_json"


@pl.api.register_expr_namespace("snowflake")
class SnowflakeExpr:
    def __init__(self, expr: pl.Expr) -> None:
        self._expr = expr

    def try_parse_json(self) -> pl.Expr:
        """Parse JSON to a Snowflake VARIANT, returning SQL NULL on invalid input."""
        return try_parse_json(self._expr)

    def try_to_number(self) -> pl.Expr:
        """Parse text to Snowflake NUMBER(38,0), returning SQL NULL on failure."""
        return _call("try_to_number", self._expr)

    def typeof(self) -> pl.Expr:
        """Return the Snowflake type of the value stored in a VARIANT."""
        return _call("typeof", self._expr)

    def get_path(self, path: str | pl.Expr) -> pl.Expr:
        """Read a path from a semi-structured value."""
        return _call("get_path", self._expr, pl.lit(path) if isinstance(path, str) else path)

    def is_null_value(self) -> pl.Expr:
        """Test for JSON null inside a Snowflake VARIANT."""
        return _call("is_null_value", self._expr)

    def array_construct(self, *values: str | pl.Expr) -> pl.Expr:
        """Create a Snowflake ARRAY starting with this expression's value."""
        return _call("array_construct", self._expr, *values)

    def object_construct_keep_null(self, value: str | pl.Expr, *pairs: str | pl.Expr) -> pl.Expr:
        """Create an OBJECT from key and value expressions, keeping null values."""
        if len(pairs) % 2:
            raise ValueError("object_construct_keep_null expects key/value pairs")
        return _call("object_construct_keep_null", self._expr, value, *pairs)


class SnowflakeColumn(pl.Expr):
    snowflake: SnowflakeExpr


type _ColumnName = str | pl.DataType | type[pl.DataType]


class _Col(Protocol):
    def __call__(
        self, name: _ColumnName | Iterable[_ColumnName], *more_names: _ColumnName
    ) -> SnowflakeColumn: ...
    def __getattr__(self, name: str) -> SnowflakeColumn: ...


col = cast(_Col, pl.col)


def try_parse_json(expr: str | pl.Expr) -> pl.Expr:
    """Parse a column or expression using Snowflake TRY_PARSE_JSON."""
    return _call(PLUGIN_SYMBOL, expr)


def _call(name: str, *args: str | pl.Expr) -> pl.Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name=name,
        args=args,
        is_elementwise=True,
        use_abs_path=True,
    )
