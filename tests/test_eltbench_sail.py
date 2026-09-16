from lakebench.benchmarks.elt_bench.engine_impl.sail import SailELTBench


class _FakeDataFrame:
    def __init__(self):
        self.write = _FakeWriter()


class _FakeWriter:
    def __init__(self):
        self.path = None

    def format(self, file_format):
        assert file_format == "delta"
        return self

    def mode(self, mode):
        assert mode == "overwrite"
        return self

    def save(self, path):
        self.path = path


class _FakeSpark:
    def __init__(self):
        self.query = None

    def sql(self, query):
        self.query = query
        return _FakeDataFrame()


class _FakeMerge:
    def when_matched_update(self, updates):
        return self

    def when_not_matched_insert(self, inserts):
        return self

    def execute(self):
        return None


class _FakeDeltaTable:
    def __init__(self, **kwargs):
        self.table_uri = kwargs["table_uri"]

    def to_pyarrow_table(self):
        return object()

    def merge(self, **kwargs):
        return _FakeMerge()


class _FakeDeltaRs:
    DeltaTable = _FakeDeltaTable


class _FakeFileSystem:
    def __init__(self):
        self.removed = []

    def exists(self, path):
        return True

    def rm(self, path, recursive):
        self.removed.append((path, recursive))


class _FakeEngine:
    def __init__(self):
        self.spark = _FakeSpark()
        self.deltars = _FakeDeltaRs()
        self.fs = _FakeFileSystem()
        self.schema_or_working_directory_uri = "/tmp/eltbench"
        self.storage_options = {}


def test_sail_elt_merge_marks_customer_id_nullable_for_projection_pushdown():
    engine = _FakeEngine()
    benchmark = SailELTBench(engine)

    benchmark.merge_percent_into_total_sales_fact(0.001)

    assert "NULLIF(" in engine.spark.query
    assert "c.c_customer_id" in engine.spark.query
    assert len(engine.fs.removed) == 1
    assert engine.fs.removed[0][0].startswith("/tmp/eltbench/_lakebench_total_sales_merge_")
    assert engine.fs.removed[0][1] is True
