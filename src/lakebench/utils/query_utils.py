def transpile_and_qualify_query(query: str, from_dialect: str, to_dialect: str, catalog: str, schema: str) -> str:
    import sqlglot as sg
    from sqlglot.optimizer.qualify_tables import qualify_tables

    expression = sg.parse_one(query, dialect=from_dialect)

    qualified_sql = qualify_tables(expression, catalog=catalog, db=schema, dialect=from_dialect).sql(
        to_dialect, normalize=False, pretty=True
    )

    return qualified_sql


def get_table_name_from_ddl(ddl: str) -> str:
    import sqlglot
    from sqlglot.expressions import Identifier, Table

    expression = sqlglot.parse_one(ddl)
    table = expression.find(Table)
    if not table or not isinstance(table.this, Identifier):
        raise ValueError("Table name not found in DDL statement.")

    return table.this.this
