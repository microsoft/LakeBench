"""``input_folder_uri`` is the preferred name; ``input_parquet_folder_uri`` is an alias."""

import inspect

import pytest

from lakebench.benchmarks.clickbench import ClickBench
from lakebench.benchmarks.elt_bench import ELTBench
from lakebench.benchmarks.tpcds import TPCDS
from lakebench.benchmarks.tpch import TPCH
from lakebench.engines.duckdb import DuckDB
from lakebench.engines.spark import Spark

from .conftest import _uninitialized_engine

URI = "file:///tmp/in"
OTHER_URI = "file:///tmp/other"

# ELTBench only registers engine-specific implementations, so it needs a Spark engine.
BENCHMARKS = [
    (TPCDS, DuckDB, {"scenario_name": "t", "scale_factor": 1000}),
    (TPCH, DuckDB, {"scenario_name": "t", "scale_factor": 1000}),
    (ClickBench, DuckDB, {"scenario_name": "t"}),
    (ELTBench, Spark, {"scenario_name": "t", "scale_factor": 1000}),
]


def _build(benchmark_class, engine_class, kwargs, **uri_kwargs):
    return benchmark_class(engine=_uninitialized_engine(engine_class), **kwargs, **uri_kwargs)


@pytest.mark.parametrize("benchmark_class,engine_class,kwargs", BENCHMARKS)
def test_either_parameter_name_sets_both_attributes(benchmark_class, engine_class, kwargs):
    for uri_kwargs in ({"input_folder_uri": URI}, {"input_parquet_folder_uri": URI}):
        benchmark = _build(benchmark_class, engine_class, kwargs, **uri_kwargs)

        assert benchmark.input_folder_uri == URI, uri_kwargs
        assert benchmark.input_parquet_folder_uri == URI, uri_kwargs


@pytest.mark.parametrize("benchmark_class,engine_class,kwargs", BENCHMARKS)
def test_matching_values_are_accepted(benchmark_class, engine_class, kwargs):
    benchmark = _build(benchmark_class, engine_class, kwargs, input_folder_uri=URI, input_parquet_folder_uri=URI)

    assert benchmark.input_folder_uri == URI


@pytest.mark.parametrize("benchmark_class,engine_class,kwargs", BENCHMARKS)
def test_conflicting_values_are_rejected(benchmark_class, engine_class, kwargs):
    with pytest.raises(ValueError, match="aliases for the same input location"):
        _build(
            benchmark_class,
            engine_class,
            kwargs,
            input_folder_uri=URI,
            input_parquet_folder_uri=OTHER_URI,
        )


@pytest.mark.parametrize("benchmark_class,_engine_class,_kwargs", BENCHMARKS)
def test_both_parameter_names_are_public(benchmark_class, _engine_class, _kwargs):
    parameters = inspect.signature(benchmark_class.__init__).parameters

    assert "input_folder_uri" in parameters
    assert "input_parquet_folder_uri" in parameters
