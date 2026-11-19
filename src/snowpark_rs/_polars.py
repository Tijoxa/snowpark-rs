"""Polars conversion for Snowflake DataFrame results."""

from __future__ import annotations

from collections.abc import Iterator
from importlib import import_module
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from ._core import DataFrame
    from ._session import Session

    try:
        from polars import DataFrame as PolarsDataFrame
    except ImportError:
        from typing import Any as PolarsDataFrame


def to_polars(
    dataframe: DataFrame, session: Session, use_batches: bool
) -> PolarsDataFrame | Iterator[PolarsDataFrame]:
    try:
        pl = import_module("polars")
    except ModuleNotFoundError as error:
        if error.name != "polars":
            raise
        raise ImportError(
            'Install "snowpark-rs[session,polars]" to use to_polars()'
        ) from error

    if use_batches:
        return map(pl.from_arrow, dataframe.to_arrow_batches(session))
    return cast("PolarsDataFrame", pl.from_arrow(dataframe.to_arrow(session)))
