from lakebench.benchmarks.elt_bench.engine_impl.sail import SailELTBench


class _FakeDataFrame:
    def toArrow(self):
        return object()


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
        pass

    def merge(self, **kwargs):
        return _FakeMerge()


class _FakeDeltaRs:
    DeltaTable = _FakeDeltaTable


class _FakeEngine:
    def __init__(self):
        self.spark = _FakeSpark()
        self.deltars = _FakeDeltaRs()
        self.schema_or_working_directory_uri = "/tmp/eltbench"
        self.storage_options = {}


def test_sail_elt_merge_keeps_customer_id_non_nullable():
    engine = _FakeEngine()
    benchmark = SailELTBench(engine)

    benchmark.merge_percent_into_total_sales_fact(0.001)

    assert "COALESCE(c.c_customer_id, '')" in engine.spark.query
