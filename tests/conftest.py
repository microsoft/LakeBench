def _uninitialized_engine(engine_class):
    """Builds an engine stub so query rendering can be tested without a runtime."""
    engine = engine_class.__new__(engine_class)
    engine.version = "test"
    engine.cost_per_vcore_hour = None
    engine.cost_per_hour = None
    engine.extended_engine_metadata = {}
    engine.storage_options = {}
    engine.schema_or_working_directory_uri = "file:///tmp/lakebench"
    engine.runtime = "local_unknown"
    engine.operating_system = "linux"
    engine.catalog_name = None
    engine.schema_name = None
    engine.get_total_cores = lambda: 1
    engine.get_compute_size = lambda: "test"
    return engine
