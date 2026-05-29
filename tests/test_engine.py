import pytest

from lakebench.engines.base import BaseEngine


class _MinimalEngine(BaseEngine):
    """Minimal concrete subclass for testing BaseEngine without optional deps."""

    def __init__(self):
        # Skip full __init__ to avoid cloud/runtime side-effects;
        # initialise only the attributes we test.
        self.version = "test"
        self.cost_per_vcore_hour = None
        self.cost_per_hour = None
        self.extended_engine_metadata = {}
        self.storage_options = {}
        self.schema_or_working_directory_uri = None
        self.fs = None
        self.runtime = "local_unknown"
        self.operating_system = self._detect_os()


class TestDetectOs:
    def test_returns_string(self):
        engine = _MinimalEngine()
        result = engine._detect_os()
        assert isinstance(result, str)

    def test_returns_known_os(self):
        engine = _MinimalEngine()
        result = engine._detect_os()
        assert result in ("windows", "linux", "mac", "unknown")


class TestGetTotalCores:
    def test_returns_positive_int(self):
        engine = _MinimalEngine()
        cores = engine.get_total_cores()
        assert isinstance(cores, int)
        assert cores > 0


class TestGetComputeSize:
    def test_format(self):
        engine = _MinimalEngine()
        size = engine.get_compute_size()
        assert isinstance(size, str)
        assert "vCore" in size

    def test_matches_core_count(self):
        engine = _MinimalEngine()
        cores = engine.get_total_cores()
        assert engine.get_compute_size() == f"{cores}vCore"


class TestGetJobCost:
    def test_returns_none_when_no_cost_set(self):
        engine = _MinimalEngine()
        assert engine.get_job_cost(60000) is None

    def test_calculates_cost_with_per_hour(self):
        engine = _MinimalEngine()
        engine.cost_per_hour = 1.0  # $1/hour
        cost = engine.get_job_cost(3600000)  # 1 hour in ms
        assert cost is not None
        assert float(cost) == pytest.approx(1.0, rel=1e-6)

    def test_calculates_cost_with_per_vcore_hour(self):
        engine = _MinimalEngine()
        engine.cost_per_vcore_hour = 0.1
        cost = engine.get_job_cost(3600000)  # 1 hour
        expected = engine.get_total_cores() * 0.1
        assert cost is not None
        assert float(cost) == pytest.approx(expected, rel=1e-6)
