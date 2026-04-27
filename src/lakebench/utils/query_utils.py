def transpile_and_qualify_query(query:str, from_dialect:str, to_dialect:str, catalog:str, schema:str)-> str:
    import sqlglot as sg
    from sqlglot.optimizer.qualify_tables import qualify_tables

    # Multi-part schema names (e.g. Fabric's workspace.lakehouse.schema, or
    # Unity Catalog's catalog.schema) need each dotted segment quoted
    # separately. sqlglot's qualify_tables takes scalar catalog/db, so we
    # split here:
    #   - rightmost segment → db
    #   - remaining segments folded back into catalog (joined with dots so
    #     they survive the AST and we can split-then-rebackquote in
    #     post-processing below).
    extra_catalog_prefix = None
    if schema and "." in schema:
        parts = schema.split(".")
        schema = parts[-1]
        if catalog:
            # Caller's catalog stays leftmost; the schema's leading segments
            # become the middle path.
            extra_catalog_prefix = catalog
            catalog = ".".join(parts[:-1])
        else:
            catalog = ".".join(parts[:-1])

    expression = sg.parse_one(query, dialect=from_dialect)

    qualified_sql = qualify_tables(
        expression,
        catalog=catalog,
        db=schema,
        dialect=from_dialect) \
    .sql(to_dialect, normalize=False, pretty=True)

    # Post-process: any catalog string we passed with embedded dots will have
    # been emitted as a single backticked identifier. Re-split it so each
    # segment is its own backticked identifier — required by Spark/Fabric for
    # 3- and 4-part names.
    if catalog and "." in catalog:
        single = f"`{catalog}`"
        multi = ".".join(f"`{seg}`" for seg in catalog.split("."))
        qualified_sql = qualified_sql.replace(single, multi)
    if extra_catalog_prefix:
        # Prepend the caller's catalog ahead of the (now multi-segment) path.
        # qualify_tables already emitted `catalog`.`schema`.`table`; we need
        # `extra_catalog_prefix`.`<that>`.
        prefix_sql = ".".join(f"`{seg}`" for seg in extra_catalog_prefix.split("."))
        catalog_head = ".".join(f"`{seg}`" for seg in catalog.split("."))
        qualified_sql = qualified_sql.replace(catalog_head + ".", prefix_sql + "." + catalog_head + ".")

    return qualified_sql

def get_table_name_from_ddl(ddl: str) -> str:
    import sqlglot
    from sqlglot.expressions import Table, Identifier

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
    from sqlglot.expressions import Create, ColumnDef, Table, Identifier

    result = {}
    for statement_text in ddl_text.split(';'):
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
            if m_col.endswith('_sk'):
                candidate = m_col[:-3]  # strip _sk
                if candidate in extra:
                    match = candidate
            # Case 2: actual has _sk suffix, DDL doesn't
            if not match and (m_col + '_sk') in extra:
                match = m_col + '_sk'
            # Case 3: DDL has _date suffix, actual doesn't (or vice versa)
            if not match and m_col.endswith('_date'):
                candidate = m_col[:-5]
                if candidate in extra:
                    match = candidate
            if not match and (m_col + '_date') in extra:
                match = m_col + '_date'
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