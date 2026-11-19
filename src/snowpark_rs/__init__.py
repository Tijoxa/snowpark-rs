from typing import TYPE_CHECKING

from ._core import (
    Column,
    DataFrame,
    Field,
    Projection,
    RelationalGroupedDataFrame,
    SortKey,
    Table,
)

if TYPE_CHECKING:
    from ._session import Session as Session

__all__ = ["Column", "DataFrame", "Field", "Projection", "RelationalGroupedDataFrame", "SortKey", "Table"]


def __getattr__(name: str) -> object:
    if name == "Session":
        from ._session import Session, require_backend

        require_backend()
        return Session
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
