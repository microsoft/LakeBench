import pytest

from lakebench.utils.query_utils import get_table_name_from_ddl, transpile_and_qualify_query


class TestTranspileAndQualifyQuery:
    def test_basic_transpile_spark_to_duckdb(self):
        query = "SELECT * FROM orders"
        result = transpile_and_qualify_query(
            query=query,
            from_dialect="spark",
            to_dialect="duckdb",
            catalog="my_catalog",
            schema="my_schema",
        )
        assert "my_catalog" in result
        assert "my_schema" in result
        assert "orders" in result

    def test_table_qualification_applied(self):
        query = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
        result = transpile_and_qualify_query(
            query=query,
            from_dialect="spark",
            to_dialect="duckdb",
            catalog="bench",
            schema="tpch",
        )
        assert "bench" in result
        assert "tpch" in result

    def test_output_is_string(self):
        query = "SELECT 1 AS col"
        result = transpile_and_qualify_query(
            query=query,
            from_dialect="spark",
            to_dialect="duckdb",
            catalog="cat",
            schema="sch",
        )
        assert isinstance(result, str)

    def test_no_catalog_no_schema(self):
        query = "SELECT * FROM lineitem"
        result = transpile_and_qualify_query(
            query=query,
            from_dialect="spark",
            to_dialect="duckdb",
            catalog=None,
            schema=None,
        )
        assert "lineitem" in result

    # ---- multi-part (3- and 4-part) name qualification ----

    def test_three_part_schema_no_catalog_spark(self):
        """Fabric-style workspace.lakehouse.schema → 4 backticked segments."""
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="spark",
            catalog=None,
            schema="ws.lakehouse.dbo",
        )
        assert "`ws`.`lakehouse`.`dbo`.`orders`" in result

    def test_catalog_plus_two_part_schema_spark(self):
        """catalog + dotted schema must NOT drop the catalog (the old bug)."""
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="spark",
            catalog="cat",
            schema="mid.sch",
        )
        assert "`cat`.`mid`.`sch`.`orders`" in result

    def test_two_part_catalog_schema_spark(self):
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="spark",
            catalog="cat",
            schema="sch",
        )
        assert "`cat`.`sch`.`orders`" in result

    def test_multi_part_applies_to_all_tables_in_join(self):
        result = transpile_and_qualify_query(
            query="SELECT a FROM orders o JOIN customers c ON o.id = c.id",
            from_dialect="spark",
            to_dialect="spark",
            catalog="cat",
            schema="mid.sch",
        )
        assert "`cat`.`mid`.`sch`.`orders`" in result
        assert "`cat`.`mid`.`sch`.`customers`" in result

    def test_non_spark_dialect_uses_bare_segments(self):
        """DuckDB et al. don't get backticks; sqlglot quotes per-dialect."""
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="duckdb",
            catalog="cat",
            schema="sch",
        )
        assert "`" not in result
        assert "cat.sch.orders" in result

    def test_cte_reference_is_not_qualified(self):
        """A CTE name must stay bare; only the real base table is qualified."""
        result = transpile_and_qualify_query(
            query="WITH t AS (SELECT * FROM orders) SELECT * FROM t",
            from_dialect="spark",
            to_dialect="spark",
            catalog=None,
            schema="db",
        )
        assert "`db`.`orders`" in result
        # The final `FROM t` must reference the CTE, not `db`.`t`.
        assert "`db`.`t`" not in result

    def test_schema_with_leading_or_trailing_dots_tolerated(self):
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="spark",
            catalog=None,
            schema="ws..dbo.",
        )
        # Empty segments are dropped.
        assert "`ws`.`dbo`.`orders`" in result

    def test_four_part_name_catalog_and_three_part_schema(self):
        result = transpile_and_qualify_query(
            query="SELECT * FROM orders",
            from_dialect="spark",
            to_dialect="spark",
            catalog="cat",
            schema="a.b.c",
        )
        assert "`cat`.`a`.`b`.`c`.`orders`" in result


class TestGetTableNameFromDdl:
    def test_simple_create_table(self):
        ddl = "CREATE TABLE orders (id INT, name STRING)"
        assert get_table_name_from_ddl(ddl) == "orders"

    def test_create_table_if_not_exists(self):
        ddl = "CREATE TABLE IF NOT EXISTS customers (id INT)"
        assert get_table_name_from_ddl(ddl) == "customers"

    def test_mixed_case_table_name(self):
        ddl = "CREATE TABLE MyTable (col1 INT)"
        result = get_table_name_from_ddl(ddl)
        assert result.lower() == "mytable"

    def test_invalid_ddl_raises(self):
        with pytest.raises(Exception):
            get_table_name_from_ddl("NOT A VALID DDL STATEMENT")
