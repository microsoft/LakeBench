def transpile_and_qualify_query(
    query: str,
    from_dialect: str,
    to_dialect: str,
    catalog: str,
    schema: str,
) -> str:
    """Transpile a query from one dialect to another and qualify its tables.

    Tables in the query are written with bare names; this prepends the engine's
    catalog/schema. Both ``catalog`` and ``schema`` may themselves be multi-part
    dotted names — e.g. Fabric's ``workspace.lakehouse.schema`` or Unity
    Catalog's ``catalog.schema`` — yielding 3- and 4-part qualified names.

    For Spark-family dialects each segment is emitted as its own quoted
    identifier (``\\`a\\`.\\`b\\`.\\`c\\`.tbl``); other dialects use bare dotted
    segments. CTE/derived-table references are left untouched because
    ``qualify_tables`` only annotates real base tables.
    """
    import sqlglot as sg
    from sqlglot import exp
    from sqlglot.optimizer.qualify_tables import qualify_tables

    tree = sg.parse_one(query, dialect=from_dialect)

    # Collect the full namespace prefix (catalog segments, then schema segments).
    prefix_segments = []
    if catalog:
        prefix_segments += [s for s in str(catalog).split(".") if s]
    if schema:
        prefix_segments += [s for s in str(schema).split(".") if s]

    if not prefix_segments:
        return tree.sql(to_dialect, normalize=False, pretty=True)

    # Qualify using only the rightmost segment as the db. This makes
    # qualify_tables annotate exactly the base tables (and skip CTEs / derived
    # tables), after which we rebuild the full multi-part prefix ourselves.
    db_marker = prefix_segments[-1]
    tree = qualify_tables(tree, db=db_marker, dialect=from_dialect)

    # Spark / Hive / Databricks need backticked identifiers for multi-part
    # names; other engines (DuckDB, Postgres, …) take bare dotted segments and
    # sqlglot will quote as its dialect requires.
    quoted = to_dialect in ("spark", "hive", "databricks")

    def _identifier(name: str) -> exp.Identifier:
        return exp.to_identifier(name, quoted=quoted)

    for table in tree.find_all(exp.Table):
        # Only rewrite the base tables we just qualified: db == db_marker and no
        # catalog yet. Anything else (already-qualified, CTE refs) is left alone.
        if table.db != db_marker or table.catalog:
            continue

        table_name = table.name
        table_alias = table.args.get("alias")

        # Build `seg1`.`seg2`.….`table` as a chained Dot expression so an
        # arbitrary number of prefix segments is supported.
        parts = [_identifier(seg) for seg in prefix_segments] + [_identifier(table_name)]
        node = parts[0]
        for part in parts[1:]:
            node = exp.Dot(this=node, expression=part)

        new_table = exp.Table(this=node)
        if table_alias is not None:
            new_table.set("alias", table_alias)
        table.replace(new_table)

    return tree.sql(to_dialect, normalize=False, pretty=True)


def get_table_name_from_ddl(ddl: str) -> str:
    import sqlglot
    from sqlglot.expressions import Identifier, Table

    expression = sqlglot.parse_one(ddl)
    table = expression.find(Table)
    if not table or not isinstance(table.this, Identifier):
        raise ValueError("Table name not found in DDL statement.")

    return table.this.this


def parse_ddl_columns(ddl_text: str) -> dict:
    """
    Parse a DDL file containing multiple CREATE TABLE statements.
    Returns {table_name: [col1, col2, ...]} with lowercased names.
    """
    import sqlglot
    from sqlglot.expressions import ColumnDef, Create, Identifier, Table

    result = {}
    for statement_text in ddl_text.split(";"):
        statement_text = statement_text.strip()
        if len(statement_text) < 8:
            continue
        try:
            expr = sqlglot.parse_one(statement_text)
            if not isinstance(expr, Create):
                continue
            table = expr.find(Table)
            if not table or not isinstance(table.this, Identifier):
                continue
            table_name = table.this.this.lower()
            columns = []
            for col_def in expr.find_all(ColumnDef):
                if isinstance(col_def.this, Identifier):
                    columns.append(col_def.this.this.lower())
            if columns:
                result[table_name] = columns
        except Exception:
            continue
    return result


def build_column_remap(ddl_columns: dict, actual_schemas: dict) -> dict:
    """
    Compare DDL-defined columns vs actual table columns and build a remap dict.

    Parameters
    ----------
    ddl_columns : dict
        {table_name: [col1, col2, ...]} from DDL (lowercased).
    actual_schemas : dict
        {table_name: [col1, col2, ...]} from engine introspection (lowercased).

    Returns
    -------
    dict
        {ddl_col_name: actual_col_name} for mismatched columns.
    """
    remap = {}
    for table_name, ddl_cols in ddl_columns.items():
        actual_cols = actual_schemas.get(table_name)
        if not actual_cols:
            continue
        actual_set = set(actual_cols)
        ddl_set = set(ddl_cols)

        # Find DDL columns missing from actual data
        missing = ddl_set - actual_set
        # Find actual columns not in DDL
        extra = actual_set - ddl_set

        for m_col in missing:
            # Try common suffix/prefix variations
            match = None
            # Case 1: DDL has _sk suffix, actual doesn't
            if m_col.endswith("_sk"):
                candidate = m_col[:-3]  # strip _sk
                if candidate in extra:
                    match = candidate
            # Case 2: actual has _sk suffix, DDL doesn't
            if not match and (m_col + "_sk") in extra:
                match = m_col + "_sk"
            # Case 3: DDL has _date suffix, actual doesn't (or vice versa)
            if not match and m_col.endswith("_date"):
                candidate = m_col[:-5]
                if candidate in extra:
                    match = candidate
            if not match and (m_col + "_date") in extra:
                match = m_col + "_date"
            # Case 4: simple Levenshtein for close matches
            if not match:
                for e_col in extra:
                    if _levenshtein_ratio(m_col, e_col) > 0.85:
                        match = e_col
                        break

            if match:
                remap[m_col] = match
                extra.discard(match)  # don't reuse

    return remap


def _levenshtein_ratio(s1: str, s2: str) -> float:
    """Compute similarity ratio between two strings (0.0 to 1.0)."""
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    # Simple Levenshtein distance
    matrix = list(range(len2 + 1))
    for i in range(1, len1 + 1):
        prev = matrix[0]
        matrix[0] = i
        for j in range(1, len2 + 1):
            temp = matrix[j]
            if s1[i - 1] == s2[j - 1]:
                matrix[j] = prev
            else:
                matrix[j] = 1 + min(prev, matrix[j], matrix[j - 1])
            prev = temp
    distance = matrix[len2]
    max_len = max(len1, len2)
    return 1.0 - (distance / max_len)


def apply_column_remap(query: str, remap: dict, dialect: str) -> str:
    """
    Apply column name remapping to a SQL query using sqlglot AST transformation.

    Parameters
    ----------
    query : str
        The SQL query string.
    remap : dict
        {old_column_name: new_column_name} mapping (lowercased keys).
    dialect : str
        The SQL dialect for parsing/generating.

    Returns
    -------
    str
        The query with column names remapped.
    """
    import sqlglot
    from sqlglot.expressions import Column

    tree = sqlglot.parse_one(query, dialect=dialect)

    for col_node in tree.find_all(Column):
        col_name = col_node.name.lower()
        if col_name in remap:
            col_node.this.set("this", remap[col_name])

    return tree.sql(dialect=dialect, normalize=False, pretty=True)
