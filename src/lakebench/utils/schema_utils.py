"""Helpers for deriving reader schemas from benchmark DDL.

The generator's native output is pipe-delimited text with no header and no
embedded types, so loading it requires an explicit schema. The benchmark's
canonical DDL is the authoritative source for column order and column types,
which keeps the native load path consistent with the Parquet load path.
"""

from typing import Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

#: Column appended to every native reader schema to absorb the empty field
#: produced by the trailing delimiter the TPC generators emit on each line.
TRAILING_DELIMITER_COLUMN = "_lakebench_trailing_delimiter"

NATIVE_DELIMITER = "|"


def table_schemas_from_ddl(ddl: str, dialect: str = "spark") -> Dict[str, List[Tuple[str, exp.DataType]]]:
    """Return ordered ``(column_name, data_type)`` pairs for each table in ``ddl``."""
    schemas: Dict[str, List[Tuple[str, exp.DataType]]] = {}
    for statement in sqlglot.parse(ddl, read=dialect):
        if not isinstance(statement, exp.Create):
            continue
        table = statement.find(exp.Table)
        schema = statement.find(exp.Schema)
        if table is None or schema is None:
            continue
        columns = [
            (column.name.lower(), column.args["kind"])
            for column in schema.expressions
            if isinstance(column, exp.ColumnDef) and column.args.get("kind") is not None
        ]
        if columns:
            schemas[table.name.lower()] = columns
    return schemas


def schema_to_sql_types(
    columns: List[Tuple[str, exp.DataType]],
    dialect: str,
    column_name_mapping: Optional[Dict[str, str]] = None,
) -> "Dict[str, str]":
    """Render ``columns`` as an ordered ``{column_name: sql_type}`` mapping."""
    return {_mapped_name(name, column_name_mapping): data_type.sql(dialect=dialect) for name, data_type in columns}


def schema_to_pyarrow(
    columns: List[Tuple[str, exp.DataType]],
    column_name_mapping: Optional[Dict[str, str]] = None,
):
    """Render ``columns`` as a :mod:`pyarrow` schema."""
    import pyarrow as pa

    return pa.schema(
        [pa.field(_mapped_name(name, column_name_mapping), _pyarrow_type(data_type)) for name, data_type in columns]
    )


def _mapped_name(name: str, column_name_mapping: Optional[Dict[str, str]]) -> str:
    if not column_name_mapping:
        return name
    return column_name_mapping.get(name, name)


def _pyarrow_type(data_type: exp.DataType):
    import pyarrow as pa

    # Compare by type name rather than enum member so the mapping works across
    # both pinned SQLGlot versions, whose Type enums differ.
    type_name = data_type.this.name if hasattr(data_type.this, "name") else str(data_type.this)

    simple_types = {
        "TINYINT": pa.int8,
        "SMALLINT": pa.int16,
        "INT": pa.int32,
        "MEDIUMINT": pa.int32,
        "BIGINT": pa.int64,
        "CHAR": pa.string,
        "NCHAR": pa.string,
        "VARCHAR": pa.string,
        "NVARCHAR": pa.string,
        "TEXT": pa.string,
        "DATE": pa.date32,
        "DOUBLE": pa.float64,
        "FLOAT": pa.float32,
        "BOOLEAN": pa.bool_,
    }
    if type_name in simple_types:
        return simple_types[type_name]()
    if type_name in ("TIMESTAMP", "TIMESTAMPNTZ", "DATETIME"):
        return pa.timestamp("us")
    if type_name in ("TIMESTAMPTZ", "TIMESTAMPLTZ"):
        return pa.timestamp("us", tz="UTC")
    if type_name in ("DECIMAL", "NUMERIC", "BIGDECIMAL"):
        precision, scale = _decimal_parameters(data_type)
        return pa.decimal128(precision, scale)

    raise ValueError(f"Unsupported DDL type for native schema derivation: {data_type.sql()}")


def _decimal_parameters(data_type: exp.DataType) -> Tuple[int, int]:
    parameters = [int(parameter.name) for parameter in data_type.expressions if parameter.name.isdigit()]
    if len(parameters) == 2:
        return parameters[0], parameters[1]
    if len(parameters) == 1:
        return parameters[0], 0
    return 38, 18
