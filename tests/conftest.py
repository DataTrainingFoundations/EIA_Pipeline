from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_snowflake_stubs() -> None:
    if "snowflake.snowpark" in sys.modules:
        return

    snowflake_module = types.ModuleType("snowflake")
    snowpark_module = types.ModuleType("snowflake.snowpark")
    functions_module = types.ModuleType("snowflake.snowpark.functions")
    types_module = types.ModuleType("snowflake.snowpark.types")

    class Session:
        class builder:
            @staticmethod
            def configs(_configs):
                class _Builder:
                    @staticmethod
                    def create():
                        return {"created": True}

                return _Builder()

    class _FakeWindowSpec:
        def order_by(self, *columns):
            return ("order_by", columns)

    class Window:
        @staticmethod
        def partition_by(*columns):
            return _FakeWindowSpec()

    class _Type:
        pass

    class DoubleType(_Type):
        pass

    class LongType(_Type):
        pass

    class StringType(_Type):
        pass

    class StructField:
        def __init__(self, name, datatype, nullable=True):
            self.name = name
            self.datatype = datatype
            self.nullable = nullable

    class StructType:
        def __init__(self, fields):
            self.fields = fields
            self.names = [field.name for field in fields]

    class _Expr:
        def __init__(self, value):
            self.value = value

        def cast(self, cast_type):
            return _Expr(("cast", self.value, cast_type))

        def alias(self, alias_name):
            return _Expr(("alias", self.value, alias_name))

        def is_not_null(self):
            return _Expr(("is_not_null", self.value))

        def substr(self, start, length):
            return _Expr(("substr", self.value, start, length))

        def desc_nulls_last(self):
            return _Expr(("desc_nulls_last", self.value))

        def isin(self, values):
            return _Expr(("isin", self.value, tuple(values)))

        def otherwise(self, other):
            return _Expr(("otherwise", self.value, other))

        def __ge__(self, other):
            return _Expr(("ge", self.value, other))

        def __gt__(self, other):
            return _Expr(("gt", self.value, other))

        def __truediv__(self, other):
            return _Expr(("div", self.value, other))

        def __mul__(self, other):
            return _Expr(("mul", self.value, other))

        def __eq__(self, other):  # type: ignore[override]
            return _Expr(("eq", self.value, other))

        def __or__(self, other):
            return _Expr(("or", self.value, getattr(other, "value", other)))

        def __repr__(self):
            return f"_Expr({self.value!r})"

    def _expr(name):
        def _inner(*args, **kwargs):
            return _Expr((name, args, kwargs))

        return _inner

    for name in (
        "col",
        "concat",
        "current_timestamp",
        "lit",
        "md5",
        "row_number",
        "to_date",
        "to_timestamp",
        "trim",
        "upper",
        "when",
        "max",
        "round",
        "sum",
    ):
        setattr(functions_module, name, _expr(name))

    snowpark_module.Session = Session
    snowpark_module.Window = Window
    types_module.DoubleType = DoubleType
    types_module.LongType = LongType
    types_module.StringType = StringType
    types_module.StructField = StructField
    types_module.StructType = StructType

    sys.modules["snowflake"] = snowflake_module
    sys.modules["snowflake.snowpark"] = snowpark_module
    sys.modules["snowflake.snowpark.functions"] = functions_module
    sys.modules["snowflake.snowpark.types"] = types_module


_install_snowflake_stubs()


class FakeRow(dict):
    def as_dict(self):
        return dict(self)


class FakeQuery:
    def __init__(self, rows=None):
        self.rows = rows or []

    def collect(self):
        return self.rows


class FakeTable:
    def __init__(self, session, name, schema_names=None):
        self.session = session
        self.name = name
        self.schema = types.SimpleNamespace(names=list(schema_names or []))

    def limit(self, _n):
        return self

    def collect(self):
        return []


class FakeSession:
    def __init__(self):
        self.sql_calls = []
        self.sql_results = []
        self.tables = {}
        self.created = []
        self.closed = False

    def sql(self, query):
        self.sql_calls.append(query)
        rows = self.sql_results.pop(0) if self.sql_results else []
        return FakeQuery(rows)

    def table(self, name):
        if name not in self.tables:
            raise RuntimeError(f"Unknown table {name}")
        value = self.tables[name]
        if isinstance(value, Exception):
            raise value
        return value

    def create_dataframe(self, rows, schema=None):
        self.created.append((rows, schema))
        return FakeDataFrame(rows, schema)

    def close(self):
        self.closed = True


class FakeWriter:
    def __init__(self):
        self.calls = []

    def mode(self, mode_name):
        self.calls.append(("mode", mode_name))
        return self

    def save_as_table(self, table_name, **kwargs):
        self.calls.append(("save_as_table", table_name, kwargs))


class FakeDataFrame:
    def __init__(self, rows=None, schema=None, count_value=None):
        self.rows = rows or []
        self.schema = schema or types.SimpleNamespace(names=[])
        self.count_value = len(self.rows) if count_value is None else count_value
        self.write = FakeWriter()
        self.operations = []

    def count(self):
        return self.count_value

    def with_column(self, name, value):
        self.operations.append(("with_column", name, value))
        names = list(getattr(self.schema, "names", []))
        if name not in names:
            names.append(name)
        self.schema = types.SimpleNamespace(names=names)
        return self

    def with_column_renamed(self, old, new):
        self.operations.append(("with_column_renamed", old, new))
        return self

    def filter(self, condition):
        self.operations.append(("filter", condition))
        return self

    def select(self, *columns):
        self.operations.append(("select", columns))
        self.schema = types.SimpleNamespace(names=[str(column) for column in columns])
        return self

    def drop(self, *columns):
        self.operations.append(("drop", columns))
        return self

    def dropna(self, subset=None):
        self.operations.append(("dropna", tuple(subset or [])))
        return self

    def drop_duplicates(self, columns):
        self.operations.append(("drop_duplicates", tuple(columns)))
        return self

    def group_by(self, *columns):
        self.operations.append(("group_by", columns))
        return FakeGroupedDataFrame(self)

    def agg(self, *args, **kwargs):
        self.operations.append(("agg", args, kwargs))
        return self

    def join(self, other, on=None, how=None):
        self.operations.append(("join", on, how))
        return self

    def union(self, other):
        self.operations.append(("union", other))
        return self


class FakeGroupedDataFrame:
    def __init__(self, df):
        self.df = df

    def agg(self, *args, **kwargs):
        self.df.operations.append(("agg", args, kwargs))
        return self.df


@pytest.fixture
def fake_session():
    return FakeSession()
