import sys
from decimal import Decimal
from types import ModuleType
from unittest.mock import Mock

import polars as pl
import pytest

import snowpark_rs
from snowpark_rs import Table, UnsupportedPolarsExpression, _session


class MockConnection:
    def __init__(self, connection_name: str | None) -> None:
        self.connection_name = connection_name
        self.to_arrow = Mock()
        self.to_arrow_batches = Mock()


@pytest.fixture
def open_session(monkeypatch):
    backend = ModuleType("_snowpark_rs_session")
    monkeypatch.setattr(backend, "Connection", MockConnection, raising=False)
    monkeypatch.setitem(sys.modules, backend.__name__, backend)

    def create(connection_name: str | None = None):
        builder = snowpark_rs.Session.builder
        if connection_name is not None:
            builder.config("connection_name", connection_name)
        session = builder.create()
        connection = session._connection
        assert isinstance(connection, MockConnection)
        return session, connection

    return create


@pytest.fixture
def frame():
    return Table("DB.SCHEMA.EXAMPLE").filter(pl.col("FLAG").eq(True))


@pytest.fixture
def arrow_result():
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema(
        [
            pa.field("AMOUNT", pa.decimal128(38, 2), nullable=False),
            pa.field("FLAG", pa.bool_()),
            pa.field("CREATED_AT_TZ", pa.timestamp("ns", tz="UTC")),
        ]
    )
    return pa.Table.from_pylist(
        [
            {
                "AMOUNT": Decimal("123456789012345678901234567890.12"),
                "FLAG": None,
                "CREATED_AT_TZ": 123_456_789,
            },
            {"AMOUNT": Decimal("-0.25"), "FLAG": True, "CREATED_AT_TZ": None},
        ],
        schema=schema,
    )


def _arrow_ipc(table):
    from pyarrow import BufferOutputStream, ipc

    sink = BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def test_offline_api_does_not_import_the_optional_backend(monkeypatch):
    def missing(name):
        raise ModuleNotFoundError(name=name)

    monkeypatch.setattr(_session, "import_module", missing)
    frame = Table("DB.SCHEMA.EXAMPLE")
    assert frame.to_sql()[1] == []
    with pytest.raises(ImportError, match=r"snowpark-rs\[session\]"):
        _ = snowpark_rs.Session


def test_unsupported_polars_expression_fails_before_using_the_session(open_session):
    session, connection = open_session()
    frame = Table("T").filter(pl.col("X").rolling_sum(2) > 10)

    with pytest.raises(
        UnsupportedPolarsExpression, match="filter: Function.RollingExpr"
    ):
        frame.to_polars(session)
    with pytest.raises(
        UnsupportedPolarsExpression, match="filter: Function.RollingExpr"
    ):
        frame.to_polars(session, use_batches=True)
    connection.to_arrow.assert_not_called()
    connection.to_arrow_batches.assert_not_called()


def test_to_arrow_uses_the_explicit_session(frame, open_session, arrow_result):
    session, connection = open_session("data")
    empty_session, empty_connection = open_session()
    empty_result = arrow_result.slice(0, 0)
    connection.to_arrow.return_value = _arrow_ipc(arrow_result)
    empty_connection.to_arrow.return_value = _arrow_ipc(empty_result)
    empty_connection.to_arrow_batches.return_value = iter(())

    assert frame.to_arrow(session).equals(arrow_result)
    empty = frame.to_arrow(empty_session)
    assert empty.num_rows == 0
    assert empty.schema == arrow_result.schema
    assert list(frame.to_arrow_batches(empty_session)) == []

    assert connection.connection_name == "data"
    assert empty_connection.connection_name is None
    query = frame.to_sql()
    connection.to_arrow.assert_called_once_with(*query)
    empty_connection.to_arrow.assert_called_once_with(*query)
    empty_connection.to_arrow_batches.assert_called_once_with(*query)


def test_schema_with_session_reads_zero_rows_and_passes_exact_binds(
    open_session, arrow_result
):
    session, connection = open_session()
    empty = arrow_result.select(["AMOUNT", "FLAG"]).slice(0, 0)
    connection.to_arrow.return_value = _arrow_ipc(empty)
    amount = Decimal("123456789012345678901234567890.12")
    frame = Table("example").select(
        AMOUNT=pl.lit(amount), FLAG=pl.lit(None, pl.Boolean)
    )

    assert frame.schema_with_session(session) == empty.schema
    sql, binds = connection.to_arrow.call_args.args
    assert sql.endswith("LIMIT 0")
    assert binds == [amount, None]
    assert type(binds[0]) is Decimal


@pytest.mark.parametrize("consume_all", [True, False], ids=["all", "early-stop"])
def test_to_arrow_batches_reads_on_demand(
    frame, open_session, arrow_result, consume_all
):
    session, connection = open_session("batches")
    first = arrow_result.slice(0, 1)
    last = arrow_result.slice(1)
    read_last = Mock(return_value=_arrow_ipc(last))
    closed = Mock()

    def read_batches(_sql, _binds):
        try:
            yield _arrow_ipc(first)
            yield read_last()
        finally:
            closed()

    connection.to_arrow_batches.side_effect = read_batches
    batches = frame.to_arrow_batches(session)

    connection.to_arrow_batches.assert_called_once_with(*frame.to_sql())
    read_last.assert_not_called()
    assert next(batches).equals(first)
    read_last.assert_not_called()

    if consume_all:
        (last_batch,) = batches
        assert last_batch.equals(last)
        read_last.assert_called_once_with()
    else:
        del batches
        read_last.assert_not_called()
    closed.assert_called_once_with()


def test_to_arrow_batches_propagates_errors_during_iteration(
    frame, open_session, arrow_result
):
    session, connection = open_session("broken")
    error = RuntimeError("result read failed for test-query")
    first = arrow_result.slice(0, 1)

    def read_batches():
        yield _arrow_ipc(first)
        raise error

    connection.to_arrow_batches.return_value = read_batches()
    batches = frame.to_arrow_batches(session)

    assert next(batches).equals(first)
    with pytest.raises(RuntimeError) as raised:
        next(batches)
    assert raised.value is error
    assert list(batches) == []


def test_to_polars_converts_materialized_and_batched_ipc(
    frame, open_session, arrow_result
):
    session, connection = open_session("polars")
    connection.to_arrow.return_value = _arrow_ipc(arrow_result)
    first = arrow_result.slice(0, 1)
    last = arrow_result.slice(1)
    read_last = Mock(return_value=_arrow_ipc(last))

    def read_batches():
        yield _arrow_ipc(first)
        yield read_last()

    connection.to_arrow_batches.return_value = read_batches()

    materialized = frame.to_polars(session)
    assert isinstance(materialized, pl.DataFrame)
    assert materialized["AMOUNT"].to_list() == arrow_result["AMOUNT"].to_pylist()
    assert materialized["FLAG"].to_list() == [None, True]
    assert materialized.schema["CREATED_AT_TZ"] == pl.Datetime("ns", "UTC")
    assert materialized["CREATED_AT_TZ"].cast(pl.Int64).to_list() == [123_456_789, None]

    batches = frame.to_polars(session, use_batches=True)
    read_last.assert_not_called()
    first_batch = next(batches)
    read_last.assert_not_called()
    (last_batch,) = batches
    read_last.assert_called_once_with()
    assert [first_batch.height, last_batch.height] == [1, 1]
    assert materialized.equals(pl.concat([first_batch, last_batch]))

    empty_session, empty_connection = open_session("empty")
    empty_connection.to_arrow.return_value = _arrow_ipc(arrow_result.slice(0, 0))
    empty_connection.to_arrow_batches.return_value = iter(())
    empty = frame.to_polars(empty_session)
    assert empty.is_empty()
    assert empty.schema == materialized.schema
    assert list(frame.to_polars(empty_session, use_batches=True)) == []
