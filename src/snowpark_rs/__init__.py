from typing import TYPE_CHECKING

from ._core import Column, DataFrame, Projection

if TYPE_CHECKING:
    from ._session import Session as Session

__all__ = ["Column", "DataFrame", "Projection"]


def __getattr__(name: str) -> object:
    if name == "Session":
        from ._session import Session, require_backend

        require_backend()
        return Session
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
