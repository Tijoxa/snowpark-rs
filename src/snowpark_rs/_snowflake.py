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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name=PLUGIN_SYMBOL,
        args=expr,
        is_elementwise=True,
        use_abs_path=True,
    )
