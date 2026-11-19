"""Lazy Snowflake tables using native Polars expressions."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from functools import cached_property, wraps
from typing import TYPE_CHECKING, Any, Concatenate, Literal, overload

import polars as pl

from ._core import Field, UnsupportedPolarsExpression, _Frame
from ._polars import to_polars as _to_polars
from ._session import to_arrow, to_arrow_batches
from ._snowflake import PLUGIN_PATH
from ._subquery import reference, referenced_frame

if TYPE_CHECKING:
    from pyarrow import Schema
    from pyarrow import Table as ArrowTable

    from ._core import BindValue
    from ._session import Session


type IntoExpr = pl.Expr | str | int | float | bool | Decimal | date | datetime | None
type Exprs = IntoExpr | Iterable[IntoExpr]
type JoinKeys = str | pl.Expr | Iterable[str | pl.Expr]
type _Build = Callable[[], _Frame]


def _expr(value: Any) -> pl.Expr:
    if isinstance(value, pl.Expr):
        return value
    if isinstance(value, str):
        return pl.col(value)
    return pl.lit(value)


def _expressions(
    args: tuple[Exprs, ...], named: dict[str, IntoExpr]
) -> tuple[pl.Expr, ...]:
    values = (
        tuple(args[0])
        if len(args) == 1
        and isinstance(args[0], Iterable)
        and not isinstance(args[0], (str, pl.Expr))
        else args
    )
    exprs = [_expr(value) for value in values]
    exprs.extend(_expr(value).alias(name) for name, value in named.items())
    return tuple(exprs)


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _unsupported(operation: str, node: str, reason: str) -> UnsupportedPolarsExpression:
    return UnsupportedPolarsExpression(f"{operation}: {node}: {reason}")


def _flags(values: bool | Sequence[bool], count: int, name: str) -> list[bool]:
    if isinstance(values, bool):
        return [values] * count
    if len(values) != count or any(not isinstance(value, bool) for value in values):
        raise ValueError(f"{name} must provide one bool per sort expression")
    return list(values)


class DataFrame:
    """An immutable lazy query that compiles against Snowflake."""

    def __init__(self, build: _Build) -> None:
        self._build = build

    @cached_property
    def _native(self) -> _Frame:
        return self._build()

    def _then(self, transform: Callable[[_Frame], _Frame]) -> DataFrame:
        return DataFrame(lambda: transform(self._native))

    @property
    def schema(self) -> list[Field] | None:
        """Ordered Snowflake fields that can be resolved offline."""
        return self._native.schema

    @property
    def columns(self) -> list[str] | None:
        """Exact Snowflake output names when the schema is available."""
        schema = self.schema
        return None if schema is None else [field.name for field in schema]

    def implode(self) -> pl.Expr:
        """Use this single-column query as the right operand of Expr.is_in()."""
        return reference(self)

    def select(self, *exprs: Exprs, **named_exprs: IntoExpr) -> DataFrame:
        selection = _expressions(exprs, named_exprs)

        return self._then(
            lambda frame: frame.polars_select(selection, str(PLUGIN_PATH))
        )

    def filter(self, *predicates: Exprs, **constraints: IntoExpr) -> DataFrame:
        conditions = _expressions(predicates, {})
        conditions += tuple(
            pl.col(name) == (value if isinstance(value, pl.Expr) else pl.lit(value))
            for name, value in constraints.items()
        )
        if not conditions:
            return self

        return self._then(
            lambda frame: frame.polars_filter(
                conditions, str(PLUGIN_PATH), referenced_frame
            )
        )

    def with_columns(self, *exprs: Exprs, **named_exprs: IntoExpr) -> DataFrame:
        changes = _expressions(exprs, named_exprs)

        return self._then(
            lambda frame: frame.polars_with_columns(changes, str(PLUGIN_PATH))
        )

    def group_by(
        self, *by: Exprs, maintain_order: bool = False, **named_by: IntoExpr
    ) -> GroupBy:
        return GroupBy(self, _expressions(by, named_by), maintain_order)

    def agg(self, *exprs: Exprs, **named_exprs: IntoExpr) -> DataFrame:
        """Run global reductions, also available through select()."""
        return self.group_by().agg(*exprs, **named_exprs)

    def sort(
        self,
        by: Exprs,
        *more_by: Exprs,
        descending: bool | Sequence[bool] = False,
        nulls_last: bool | Sequence[bool] = False,
        maintain_order: bool = False,
    ) -> DataFrame:
        keys = _expressions((by, *more_by), {})
        directions = descending if isinstance(descending, bool) else tuple(descending)
        nulls = nulls_last if isinstance(nulls_last, bool) else tuple(nulls_last)

        def compile_sort(frame: _Frame) -> _Frame:
            if maintain_order:
                raise _unsupported(
                    "sort",
                    "maintain_order",
                    "Snowflake tables have no implicit tie order",
                )
            sort_keys = list(
                zip(
                    keys,
                    _flags(directions, len(keys), "descending"),
                    _flags(nulls, len(keys), "nulls_last"),
                    strict=True,
                )
            )
            return frame.polars_sort(sort_keys, str(PLUGIN_PATH))

        return self._then(compile_sort)

    def limit(self, n: int = 5, *, offset: int = 0) -> DataFrame:
        if n < 0 or offset < 0:
            raise ValueError("limit and offset must be non-negative")
        return self._then(lambda frame: frame.limit(n, offset))

    def rename(self, mapping: Mapping[str, str], *, strict: bool = True) -> DataFrame:
        names = tuple(mapping.items())

        def compile_rename(frame: _Frame) -> _Frame:
            schema = frame.schema
            if schema is None:
                raise _unsupported(
                    "rename",
                    "schema",
                    "declare the source schema or select named columns first",
                )
            columns = {field.name for field in schema}
            if strict:
                missing = [name for name, _ in names if name not in columns]
                if missing:
                    raise _unsupported(
                        "rename", "Column", f"{missing!r} absent from the input schema"
                    )
            return frame.rename(
                [
                    (_identifier(old), _identifier(new))
                    for old, new in names
                    if old in columns
                ]
            )

        return self._then(compile_rename)

    def unique(self) -> DataFrame:
        return self._then(_Frame.distinct)

    def union(self, other: DataFrame) -> DataFrame:
        return self._then(lambda frame: frame.union(other._native, False, False, False))

    def union_all(self, other: DataFrame) -> DataFrame:
        return self._then(lambda frame: frame.union(other._native, True, False, False))

    def union_by_name(
        self, other: DataFrame, *, allow_missing_columns: bool = False
    ) -> DataFrame:
        return self._then(
            lambda frame: frame.union(other._native, False, True, allow_missing_columns)
        )

    def union_all_by_name(
        self, other: DataFrame, *, allow_missing_columns: bool = False
    ) -> DataFrame:
        return self._then(
            lambda frame: frame.union(other._native, True, True, allow_missing_columns)
        )

    def join(
        self,
        other: DataFrame,
        on: JoinKeys | None = None,
        how: Literal["inner", "left", "right", "full"] = "inner",
        *,
        left_on: JoinKeys | None = None,
        right_on: JoinKeys | None = None,
        suffix: str = "_right",
        nulls_equal: bool = False,
        coalesce: bool | None = None,
    ) -> DataFrame:
        if on is not None and left_on is None and right_on is None:
            left_keys = right_keys = _expressions((on,), {})
        elif on is None and left_on is not None and right_on is not None:
            left_keys = _expressions((left_on,), {})
            right_keys = _expressions((right_on,), {})
        else:
            raise ValueError("join requires on, or both left_on and right_on")

        return self._then(
            lambda frame: _join(
                frame,
                other._native,
                left_keys,
                right_keys,
                how,
                suffix,
                nulls_equal,
                coalesce,
            )
        )

    def pipe[**P, T](
        self,
        function: Callable[Concatenate[DataFrame, P], T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return function(self, *args, **kwargs)

    def to_sql(self) -> tuple[str, list[BindValue]]:
        """Compile Snowflake SQL and positional binds without a session."""
        return self._native.to_sql()

    def schema_with_session(self, session: Session) -> Schema:
        return self.limit(0).to_arrow(session).schema

    def to_arrow(self, session: Session) -> ArrowTable:
        return to_arrow(session, self.to_sql())

    def to_arrow_batches(self, session: Session) -> Iterator[ArrowTable]:
        return to_arrow_batches(session, self.to_sql())

    @overload
    def to_polars(
        self, session: Session, *, use_batches: Literal[False] = False
    ) -> pl.DataFrame: ...
    @overload
    def to_polars(
        self, session: Session, *, use_batches: Literal[True]
    ) -> Iterator[pl.DataFrame]: ...
    @overload
    def to_polars(
        self, session: Session, *, use_batches: bool
    ) -> pl.DataFrame | Iterator[pl.DataFrame]: ...
    def to_polars(
        self, session: Session, *, use_batches: bool = False
    ) -> pl.DataFrame | Iterator[pl.DataFrame]:
        return _to_polars(self, session, use_batches)


class Table(DataFrame):
    """Start a query over a named Snowflake table."""

    def __init__(
        self, name: str | Iterable[str], *, schema: Iterable[Field] | None = None
    ) -> None:
        frame = _Frame(name, list(schema) if schema is not None else None)
        super().__init__(lambda: frame)


class GroupBy:
    def __init__(
        self, frame: DataFrame, keys: tuple[pl.Expr, ...], maintain_order: bool
    ) -> None:
        self._frame = frame
        self._keys = keys
        self._maintain_order = maintain_order

    def agg(self, *aggs: Exprs, **named_aggs: IntoExpr) -> DataFrame:
        aggregates = _expressions(aggs, named_aggs)

        def compile_group(frame: _Frame) -> _Frame:
            if self._maintain_order:
                raise _unsupported(
                    "group_by",
                    "maintain_order",
                    "Snowflake tables have no implicit group order",
                )
            return frame.polars_aggregate(self._keys, aggregates, str(PLUGIN_PATH))

        return self._frame._then(compile_group)


def _join(
    left: _Frame,
    right: _Frame,
    left_keys: tuple[pl.Expr, ...],
    right_keys: tuple[pl.Expr, ...],
    how: str,
    suffix: str,
    nulls_equal: bool,
    coalesce: bool | None,
) -> _Frame:
    left_fields = left.schema
    right_fields = right.schema
    if left_fields is None or right_fields is None:
        raise _unsupported(
            "join", "schema", "both join inputs require known column names"
        )
    left_by = left.polars_join_keys(left_keys)
    right_by = right.polars_join_keys(right_keys)
    if not left_by or len(left_by) != len(right_by):
        raise _unsupported(
            "join", "join keys", "both sides need the same nonzero number of keys"
        )

    # Use disjoint internal names so the output order, coalescing and suffixes
    # follow the Polars API independently of the SQL compiler's join defaults.
    left_names = {
        field.name: f"_left_{index}" for index, field in enumerate(left_fields)
    }
    right_names = {
        field.name: f"_right_{index}" for index, field in enumerate(right_fields)
    }
    join_left = left.rename(
        [(_identifier(name), _identifier(temp)) for name, temp in left_names.items()]
    )
    join_right = right.rename(
        [(_identifier(name), _identifier(temp)) for name, temp in right_names.items()]
    )
    keys = [
        (_identifier(left_names[l]), _identifier(right_names[r]))
        for l, r in zip(left_by, right_by, strict=True)
    ]
    joined = join_left.join(join_right, keys, how, nulls_equal)

    drop_keys = coalesce if coalesce is not None else how != "full"
    output = {
        name: temp
        for name, temp in left_names.items()
        if not (drop_keys and how == "right" and name in left_by)
    }
    left_output = set(output)
    right_join_names = set(right_by)
    for name, temp in right_names.items():
        if drop_keys and how != "right" and name in right_join_names:
            continue
        alias = name + suffix if name in left_output else name
        if alias in output:
            raise _unsupported(
                "join", "suffix", f"duplicate join output column {alias!r}"
            )
        output[alias] = temp
    full_keys = (
        {l: right_names[r] for l, r in zip(left_by, right_by, strict=True)}
        if drop_keys and how == "full"
        else {}
    )
    projections = [
        (
            _identifier(temp),
            _identifier(full_keys[name]) if name in full_keys else None,
            _identifier(name),
        )
        for name, temp in output.items()
    ]
    return joined.select_join_output(projections)


# Polars 2.1 accepts Expr, Series and scalars through its own coercion path.
# Convert a Snowflake frame before that path sees it and delegate other inputs.
def _install_table_membership() -> None:
    polars_is_in = pl.Expr.is_in

    @wraps(polars_is_in)
    def is_in(self: pl.Expr, other: Any, *, nulls_equal: bool = False) -> pl.Expr:
        if isinstance(other, DataFrame):
            other = other.implode()
        return polars_is_in(self, other, nulls_equal=nulls_equal)

    pl.Expr.is_in = is_in  # ty: ignore[invalid-assignment] -- add Snowflake frames to Polars coercion


_install_table_membership()
