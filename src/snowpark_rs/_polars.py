"""Polars conversion for Snowflake DataFrame results."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, cast

import polars as pl

if TYPE_CHECKING:
    from ._frame import DataFrame
    from ._session import Session


def to_polars(
    dataframe: DataFrame, session: Session, use_batches: bool
) -> pl.DataFrame | Iterator[pl.DataFrame]:
    if use_batches:
        return (
            cast("pl.DataFrame", pl.from_arrow(table))
            for table in dataframe.to_arrow_batches(session)
        )
    return cast("pl.DataFrame", pl.from_arrow(dataframe.to_arrow(session)))
