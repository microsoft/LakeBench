from unittest.mock import MagicMock

import pytest

from lakebench.benchmarks.elt_bench import ELTBench
from lakebench.benchmarks.tpcds import TPCDS
from lakebench.engines.base import BaseEngine
from lakebench.engines.spark import Spark


class TestResolveColumnNameMapping:
    def test_eltbench_uses_tpcds_input_aliases(self):
        assert ELTBench.COLUMN_NAME_MAPPING_REGISTRY is TPCDS.COLUMN_NAME_MAPPING_REGISTRY

    def test_empty_mapping_is_noop(self):
        assert BaseEngine._resolve_column_name_mapping("customer", ["c_customer_sk"], None) == {}

    @pytest.mark.parametrize(
        ("table_name", "legacy_name", "canonical_name"),
        [
            ("catalog_returns", "cr_return_amount_inc_tax", "cr_return_amt_inc_tax"),
            ("income_band", "ib_income_band_id", "ib_income_band_sk"),
            ("reason", "r_reason_description", "r_reason_desc"),
            ("store", "s_tax_precentage", "s_tax_percentage"),
            ("web_returns", "wr_store_credit", "wr_account_credit"),
        ],
    )
    def test_legacy_name_is_mapped(self, table_name, legacy_name, canonical_name):
        mapping = {legacy_name: canonical_name}

        assert BaseEngine._resolve_column_name_mapping(table_name, [legacy_name], mapping) == mapping

    @pytest.mark.parametrize(
        ("table_name", "mapping"),
        TPCDS.COLUMN_NAME_MAPPING_REGISTRY.items(),
    )
    def test_canonical_name_is_accepted(self, table_name, mapping):
        canonical_name = next(iter(mapping.values()))

        assert BaseEngine._resolve_column_name_mapping(table_name, [canonical_name], mapping) == {}

    def test_both_names_are_rejected(self):
        mapping = {"s_tax_precentage": "s_tax_percentage"}

        with pytest.raises(ValueError, match="both legacy column.*canonical column"):
            BaseEngine._resolve_column_name_mapping(
                "store",
                ["s_tax_precentage", "s_tax_percentage"],
                mapping,
            )

    def test_missing_names_are_rejected(self):
        mapping = {"s_tax_precentage": "s_tax_percentage"}

        with pytest.raises(ValueError, match="neither is present"):
            BaseEngine._resolve_column_name_mapping("store", ["s_store_sk"], mapping)


class _FakeWriter:
    def __init__(self):
        self.inserted_table = None

    def insertInto(self, table_name, overwrite=False):
        self.inserted_table = (table_name, overwrite)


class _FakeDataFrame:
    def __init__(self, columns):
        self.columns = columns
        self.renames = {}
        self.write = _FakeWriter()

    def withColumnsRenamed(self, mapping):
        self.renames.update(mapping)
        self.columns = [mapping.get(column, column) for column in self.columns]
        return self


class TestSparkColumnNameMapping:
    def test_renames_legacy_column_before_insert(self):
        engine = object.__new__(Spark)
        dataframe = _FakeDataFrame(["s_store_sk", "s_tax_precentage"])
        engine.spark = MagicMock()
        engine.spark.read.parquet.return_value = dataframe
        engine.run_analyze_after_load = False

        engine.load_parquet_to_delta(
            parquet_folder_uri="/input/store",
            table_name="store",
            table_is_precreated=True,
            column_name_mapping={"s_tax_precentage": "s_tax_percentage"},
        )

        assert dataframe.renames == {"s_tax_precentage": "s_tax_percentage"}
        assert dataframe.write.inserted_table == ("store", False)

    def test_leaves_canonical_column_unchanged(self):
        engine = object.__new__(Spark)
        dataframe = _FakeDataFrame(["s_store_sk", "s_tax_percentage"])
        engine.spark = MagicMock()
        engine.spark.read.parquet.return_value = dataframe
        engine.run_analyze_after_load = False

        engine.load_parquet_to_delta(
            parquet_folder_uri="/input/store",
            table_name="store",
            table_is_precreated=True,
            column_name_mapping={"s_tax_precentage": "s_tax_percentage"},
        )

        assert dataframe.renames == {}
        assert dataframe.write.inserted_table == ("store", False)
