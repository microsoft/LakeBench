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
