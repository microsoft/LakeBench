from __future__ import annotations
from typing import Optional
from ..base import BaseBenchmark
from ...utils.query_utils import transpile_and_qualify_query, get_table_name_from_ddl

from .engine_impl.spark import SparkTPCDI
from .engine_impl.duckdb import DuckDBTPCDI
from .engine_impl.daft import DaftTPCDI
from .engine_impl.polars import PolarsTPCDI
from .engine_impl.sail import SailTPCDI

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars
from ...engines.sail import Sail

import importlib.resources
import posixpath
import csv
import os


class TPCDI(BaseBenchmark):
    """
    Class for running the TPC-DI (Data Integration) benchmark.

    The TPC-DI benchmark evaluates end-to-end ETL/ELT performance across heterogeneous
    data sources. It covers data ingestion from CSV, pipe-delimited, XML, and fixed-width
    files, followed by dimensional model construction (SCD Type 1 & 2), incremental batch
    processing with CDC/merge logic, and audit validation against expected row counts.

    The benchmark implements four phases:
    1. Historical Load — ingest Batch1 source files into staging tables
    2. Dimensional Transform — build the target star schema (dimensions + facts)
    3. Incremental Updates — process Batch2/Batch3 with SCD-2 merges
    4. Audit/Validation — verify row counts against TPC-DI audit data

    Parameters
    ----------
    engine : BaseEngine
        The engine to use for executing the benchmark.
    scenario_name : str
        The name of the benchmark scenario.
    scale_factor : int, optional
        The TPC-DI scale factor used for data generation.
    input_batch_folder_uri : str, optional
        Path to the TPC-DI data generator output root directory containing
        Batch1/, Batch2/, Batch3/ subdirectories.
    result_table_uri : str, optional
        Table URI where results will be saved. Must be specified if `save_results` is True.
    save_results : bool, optional
        Whether to save the benchmark results.

    Methods
    -------
    run(mode='full')
        Runs the benchmark. Modes: 'full' (all 4 phases), 'historical_only' (Batch1 only).
    """

    BENCHMARK_IMPL_REGISTRY = {
        Spark: SparkTPCDI,
        DuckDB: DuckDBTPCDI,
        Daft: DaftTPCDI,
        Polars: PolarsTPCDI,
        Sail: SailTPCDI
    }
    MODE_REGISTRY = ['full', 'historical_only']

    # Staging tables loaded from raw source files
    STAGING_TABLE_REGISTRY = [
        'staging_status_type', 'staging_tax_rate', 'staging_trade_type',
        'staging_industry', 'staging_hr', 'staging_prospect',
        'staging_daily_market', 'staging_watch_history',
        'staging_trade', 'staging_trade_history', 'staging_cash_transaction',
        'staging_customer', 'staging_account',
        'staging_finwire_cmp', 'staging_finwire_sec', 'staging_finwire_fin'
    ]

    # Target dimensional model tables
    DIM_TABLE_REGISTRY = [
        'dim_date', 'dim_time', 'dim_status_type', 'dim_tax_rate',
        'dim_trade_type', 'dim_broker', 'dim_customer', 'dim_account',
        'dim_company', 'dim_security', 'dim_trade'
    ]

    FACT_TABLE_REGISTRY = [
        'fact_market_history', 'fact_watches', 'fact_cash_balances', 'fact_holdings'
    ]

    OTHER_TABLE_REGISTRY = [
        'financial', 'prospect', 'di_messages'
    ]

    TABLE_REGISTRY = STAGING_TABLE_REGISTRY + DIM_TABLE_REGISTRY + FACT_TABLE_REGISTRY + OTHER_TABLE_REGISTRY

    DDL_FILE_NAME = 'ddl_v1.1.0.sql'
    VERSION = '1.1.0'

    # Source file definitions: (filename, format, delimiter, target_staging_table)
    BATCH1_SOURCE_FILES = [
        ('StatusType.txt', 'delimited', '|', 'staging_status_type'),
        ('TaxRate.txt', 'delimited', '|', 'staging_tax_rate'),
        ('TradeType.txt', 'delimited', '|', 'staging_trade_type'),
        ('Industry.txt', 'delimited', '|', 'staging_industry'),
        ('HR.csv', 'csv', ',', 'staging_hr'),
        ('Prospect.txt', 'delimited', '|', 'staging_prospect'),
        ('DailyMarket.txt', 'delimited', '|', 'staging_daily_market'),
        ('WatchHistory.txt', 'delimited', '|', 'staging_watch_history'),
        ('Trade.txt', 'delimited', '|', 'staging_trade'),
        ('TradeHistory.txt', 'delimited', '|', 'staging_trade_history'),
        ('CashTransaction.txt', 'delimited', '|', 'staging_cash_transaction'),
    ]

    # These need special parsing (XML, fixed-width, CDC)
    BATCH1_SPECIAL_FILES = [
        ('CustomerMgmt.xml', 'xml', 'staging_customer', 'staging_account'),
        ('FINWIRE', 'fixed_width', 'staging_finwire_cmp', 'staging_finwire_sec', 'staging_finwire_fin'),
    ]

    # Incremental batch source files (Batch2, Batch3)
    INCREMENTAL_SOURCE_FILES = [
        ('Prospect.txt', 'delimited', '|', 'staging_prospect'),
        ('DailyMarket.txt', 'delimited', '|', 'staging_daily_market'),
        ('WatchHistory.txt', 'delimited', '|', 'staging_watch_history'),
        ('Trade.txt', 'delimited', '|', 'staging_trade'),
        ('TradeHistory.txt', 'delimited', '|', 'staging_trade_history'),
        ('CashTransaction.txt', 'delimited', '|', 'staging_cash_transaction'),
        ('Customer.txt', 'delimited', '|', 'staging_customer'),
        ('Account.txt', 'delimited', '|', 'staging_account'),
    ]

    def __init__(
            self,
            engine: BaseEngine,
            scenario_name: str,
            scale_factor: Optional[int] = None,
            input_batch_folder_uri: Optional[str] = None,
            result_table_uri: Optional[str] = None,
            save_results: bool = False,
            run_id: Optional[str] = None
            ):
        self.scale_factor = scale_factor
        self.input_batch_folder_uri = input_batch_folder_uri
        super().__init__(engine, scenario_name, input_batch_folder_uri, result_table_uri, save_results, run_id)

        for base_engine, benchmark_impl in self.BENCHMARK_IMPL_REGISTRY.items():
            if isinstance(engine, base_engine):
                self.benchmark_impl_class = benchmark_impl
                if self.benchmark_impl_class is None:
                    raise ValueError(
                        f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                        f"in benchmark '{self.__class__.__name__}'."
                    )
                break
        else:
            raise ValueError(
                f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                f"in benchmark '{self.__class__.__name__}'."
            )

        self.engine = engine
        self.scenario_name = scenario_name
        self.benchmark_impl = self.benchmark_impl_class(self.engine)

    def run(self, mode: str = 'full'):
        """
        Executes the TPC-DI benchmark.

        Parameters
        ----------
        mode : str, optional
            'full': Runs all phases — historical load, dimensional transform,
                    incremental updates (Batch2 & Batch3), and audit validation.
            'historical_only': Runs only the historical load and dimensional transform.
        """
        if mode == 'full':
            self.mode = 'full'
            self._prepare_schema()
            self._load_historical()
            self._transform_dimensional(batch_id=1)
            self._validate(batch_id=1)
            for batch_id in [2, 3]:
                self._load_incremental(batch_id)
                self._transform_incremental(batch_id)
                self._validate(batch_id)
            self.post_results()
        elif mode == 'historical_only':
            self.mode = 'historical_only'
            self._prepare_schema()
            self._load_historical()
            self._transform_dimensional(batch_id=1)
            self._validate(batch_id=1)
            self.post_results()
        else:
            raise ValueError(f"Mode '{mode}' is not supported. Supported modes: {self.MODE_REGISTRY}.")

    def _prepare_schema(self):
        """Create all target tables from DDL."""
        if not self.engine.SUPPORTS_SCHEMA_PREP:
            return

        self.engine.create_schema_if_not_exists(drop_before_create=True)
        self.engine.create_external_location(self.input_batch_folder_uri)

        engine_class_name = self.engine.__class__.__name__.lower()
        parent_class_name = self.engine.__class__.__bases__[0].__name__.lower()
        benchmark_name = 'tpcdi'
        engine_root_lib_name = self.engine.__class__.__module__.split('.')[0]
        from_dialect = self.engine.SQLGLOT_DIALECT

        try:
            with importlib.resources.path(
                f"{engine_root_lib_name}.benchmarks.{benchmark_name}.resources.ddl.{engine_class_name}",
                self.DDL_FILE_NAME
            ) as ddl_path:
                with open(ddl_path, 'r') as ddl_file:
                    ddl = ddl_file.read()
        except (ModuleNotFoundError, FileNotFoundError):
            try:
                with importlib.resources.path(
                    f"lakebench.benchmarks.{benchmark_name}.resources.ddl.{parent_class_name}",
                    self.DDL_FILE_NAME
                ) as ddl_path:
                    with open(ddl_path, 'r') as ddl_file:
                        ddl = ddl_file.read()
            except (ModuleNotFoundError, FileNotFoundError):
                with importlib.resources.path(
                    f"lakebench.benchmarks.{benchmark_name}.resources.ddl.canonical",
                    self.DDL_FILE_NAME
                ) as ddl_path:
                    with open(ddl_path, 'r') as ddl_file:
                        ddl = ddl_file.read()
                from_dialect = 'spark'

        statements = [s for s in ddl.split(';') if len(s) > 7]
        for statement in statements:
            prepped_ddl = transpile_and_qualify_query(
                query=statement,
                from_dialect=from_dialect,
                to_dialect=self.engine.SQLGLOT_DIALECT,
                catalog=getattr(self.engine, 'catalog_name', None),
                schema=getattr(self.engine, 'schema_name', None)
            )
            table_name = get_table_name_from_ddl(prepped_ddl)
            if table_name in self.TABLE_REGISTRY:
                self.engine._create_empty_table(table_name=table_name, ddl=prepped_ddl)

    def _load_historical(self):
        """Phase 1: Load Batch1 source files into staging tables."""
        batch1_uri = posixpath.join(self.input_batch_folder_uri, 'Batch1')

        # Load standard delimited files
        for filename, fmt, delimiter, staging_table in self.BATCH1_SOURCE_FILES:
            file_uri = posixpath.join(batch1_uri, filename)
            with self.timer(
                phase="Historical Load (delimited files)",
                test_item=staging_table,
                engine=self.engine
            ) as tc:
                tc.execution_telemetry = self.benchmark_impl.load_source_file(
                    file_uri=file_uri,
                    file_format=fmt,
                    delimiter=delimiter,
                    table_name=staging_table,
                    context_decorator=tc.context_decorator
                )

        # Load Date.txt and Time.txt directly into dim tables
        with self.timer(phase="Historical Load (dim_date)", test_item='dim_date', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.load_dim_date(
                file_uri=posixpath.join(batch1_uri, 'Date.txt'),
                context_decorator=tc.context_decorator
            )

        with self.timer(phase="Historical Load (dim_time)", test_item='dim_time', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.load_dim_time(
                file_uri=posixpath.join(batch1_uri, 'Time.txt'),
                context_decorator=tc.context_decorator
            )

        # Load CustomerMgmt.xml (special XML parsing)
        with self.timer(phase="Historical Load (CustomerMgmt XML)", test_item='staging_customer', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.parse_customer_mgmt_xml(
                file_uri=posixpath.join(batch1_uri, 'CustomerMgmt.xml'),
                context_decorator=tc.context_decorator
            )

        # Load FINWIRE fixed-width files
        with self.timer(phase="Historical Load (FINWIRE fixed-width)", test_item='staging_finwire', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.parse_finwire(
                batch_uri=batch1_uri,
                context_decorator=tc.context_decorator
            )

        # Load BatchDate
        with self.timer(phase="Historical Load (BatchDate)", test_item='batch_date', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.load_batch_date(
                file_uri=posixpath.join(batch1_uri, 'BatchDate.txt'),
                batch_id=1,
                context_decorator=tc.context_decorator
            )

    def _transform_dimensional(self, batch_id: int):
        """Phase 2: Build dimensional model from staging tables."""

        # Lookup dimensions (direct copies)
        for dim_table in ['dim_status_type', 'dim_tax_rate', 'dim_trade_type']:
            with self.timer(phase="Dimensional Transform (lookup)", test_item=dim_table, engine=self.engine) as tc:
                tc.execution_telemetry = self.benchmark_impl.build_lookup_dimension(
                    dim_table, batch_id=batch_id, context_decorator=tc.context_decorator
                )

        # SCD dimensions
        with self.timer(phase="Dimensional Transform", test_item='dim_broker', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_broker(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='dim_company', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_company(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='dim_security', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_security(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='dim_customer', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_customer(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='dim_account', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_account(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='dim_trade', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_trade(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        # Fact tables
        with self.timer(phase="Dimensional Transform", test_item='fact_market_history', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_market_history(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='fact_watches', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_watches(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='fact_cash_balances', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_cash_balances(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='fact_holdings', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_holdings(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        # Other tables
        with self.timer(phase="Dimensional Transform", test_item='financial', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_financial(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(phase="Dimensional Transform", test_item='prospect', engine=self.engine) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_prospect(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

    def _load_incremental(self, batch_id: int):
        """Phase 3: Load incremental batch files into staging tables."""
        batch_uri = posixpath.join(self.input_batch_folder_uri, f'Batch{batch_id}')

        for filename, fmt, delimiter, staging_table in self.INCREMENTAL_SOURCE_FILES:
            file_uri = posixpath.join(batch_uri, filename)
            with self.timer(
                phase=f"Incremental Load (Batch{batch_id})",
                test_item=staging_table,
                engine=self.engine
            ) as tc:
                tc.execution_telemetry = self.benchmark_impl.load_source_file(
                    file_uri=file_uri,
                    file_format=fmt,
                    delimiter=delimiter,
                    table_name=staging_table,
                    context_decorator=tc.context_decorator
                )

        # Load BatchDate for this batch
        with self.timer(
            phase=f"Incremental Load (Batch{batch_id})",
            test_item='batch_date',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.load_batch_date(
                file_uri=posixpath.join(batch_uri, 'BatchDate.txt'),
                batch_id=batch_id,
                context_decorator=tc.context_decorator
            )

    def _transform_incremental(self, batch_id: int):
        """Phase 3 continued: Apply incremental changes via SCD-2 merges."""

        # Merge incremental changes into SCD dimensions
        for dim_table in ['dim_customer', 'dim_account']:
            with self.timer(
                phase=f"Incremental Merge (Batch{batch_id})",
                test_item=dim_table,
                engine=self.engine
            ) as tc:
                tc.execution_telemetry = self.benchmark_impl.merge_incremental_scd2(
                    table_name=dim_table,
                    batch_id=batch_id,
                    context_decorator=tc.context_decorator
                )

        # Rebuild fact tables for incremental batch
        with self.timer(
            phase=f"Incremental Transform (Batch{batch_id})",
            test_item='dim_trade',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_dim_trade(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(
            phase=f"Incremental Transform (Batch{batch_id})",
            test_item='fact_market_history',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_market_history(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(
            phase=f"Incremental Transform (Batch{batch_id})",
            test_item='fact_watches',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_watches(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(
            phase=f"Incremental Transform (Batch{batch_id})",
            test_item='fact_cash_balances',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_fact_cash_balances(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        with self.timer(
            phase=f"Incremental Transform (Batch{batch_id})",
            test_item='prospect',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.build_prospect(
                batch_id=batch_id, context_decorator=tc.context_decorator
            )

        # Optimize and vacuum after incremental merge
        with self.timer(
            phase=f"Maintenance (Batch{batch_id})",
            test_item='OPTIMIZE',
            engine=self.engine
        ) as tc:
            for table in ['dim_customer', 'dim_account', 'dim_trade']:
                self.engine.optimize_table(table)

        with self.timer(
            phase=f"Maintenance (Batch{batch_id})",
            test_item='VACUUM',
            engine=self.engine
        ) as tc:
            for table in ['dim_customer', 'dim_account', 'dim_trade']:
                self.engine.vacuum_table(table, retain_hours=0, retention_check=False)

    def _validate(self, batch_id: int):
        """Phase 4: Validate DW tables against TPC-DI audit data."""
        audit_file = posixpath.join(
            self.input_batch_folder_uri,
            f'Batch{batch_id}_audit.csv'
        )

        with self.timer(
            phase=f"Audit Validation (Batch{batch_id})",
            test_item='audit_check',
            engine=self.engine
        ) as tc:
            tc.execution_telemetry = self.benchmark_impl.validate_audit(
                audit_file_uri=audit_file,
                batch_id=batch_id,
                context_decorator=tc.context_decorator
            )
