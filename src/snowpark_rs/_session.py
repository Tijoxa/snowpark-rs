"""Optional Snowflake sessions and conversion of native Arrow results."""

from __future__ import annotations

from collections.abc import Iterator
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from ._core import BindValue

    try:
        from pyarrow import Table
    except ImportError:
        from typing import Any as Table


def require_backend() -> ModuleType:
    try:
        return import_module("_snowpark_rs_session")
    except ModuleNotFoundError as error:
        if error.name != "_snowpark_rs_session":
            raise
        raise ImportError('Install "snowpark-rs[session]" to use Snowflake sessions') from error


class _SessionBuilder:
    def __init__(self) -> None:
        self._connection_name: str | None = None

    def config(self, key: str, value: str) -> _SessionBuilder:
        if key != "connection_name":
            raise ValueError(f"Unsupported session configuration key {key!r}")
        self._connection_name = value
        return self

    def create(self) -> Session:
        backend = require_backend()
        connection_type = getattr(backend, "Connection")  # noqa: B009 -- defined by the extension
        return Session(connection_type(self._connection_name))


class _Builder:
    def __get__(self, instance: object, owner: type[Session]) -> _SessionBuilder:
        return _SessionBuilder()


class Session:
    """A Snowflake connection opened by the Rust connector."""

    builder = _Builder()

    def __init__(self, connection: object) -> None:
        self._connection = connection


class _NativeConnection(Protocol):
    def to_arrow(self, sql: str, binds: list[BindValue]) -> bytes: ...

    def to_arrow_batches(self, sql: str, binds: list[BindValue]) -> Iterator[bytes]: ...


def _connection(session: Session) -> _NativeConnection:
    backend = require_backend()
    connection_type = getattr(backend, "Connection")  # noqa: B009 -- defined by the extension
    if not isinstance(session, Session) or not isinstance(session._connection, connection_type):
        raise TypeError("session must be a snowpark_rs.Session")
    return cast(_NativeConnection, session._connection)


def _read_table(data: bytes) -> Table:
    ipc = import_module("pyarrow.ipc")
    with ipc.open_stream(data) as reader:
        return reader.read_all()


def to_arrow(session: Session, query: tuple[str, list[BindValue]]) -> Table:
    return _read_table(_connection(session).to_arrow(*query))


def to_arrow_batches(
    session: Session, query: tuple[str, list[BindValue]]
) -> Iterator[Table]:
    batches = _connection(session).to_arrow_batches(*query)
    return map(_read_table, batches)
