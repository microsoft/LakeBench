import pytest

from lakebench.engines.fabric_spark import FabricSpark
from lakebench.engines.hdi_spark import HDISpark
from lakebench.engines.spark import Spark
from lakebench.engines.synapse_spark import SynapseSpark


class _ParentInitCalled(Exception):
    pass


@pytest.mark.parametrize(
    ("engine_class", "constructor_kwargs"),
    [
        (
            FabricSpark,
            {
                "lakehouse_name": "lakehouse",
                "lakehouse_schema_name": "schema",
            },
        ),
        (HDISpark, {"schema_name": "schema"}),
        (SynapseSpark, {"schema_name": "schema"}),
    ],
)
def test_spark_subclasses_forward_tblproperties(monkeypatch, engine_class, constructor_kwargs):
    tblproperties = {"delta.enableDeletionVectors": "false"}
    captured_kwargs = {}

    def capture_parent_init(self, **kwargs):
        captured_kwargs.update(kwargs)
        raise _ParentInitCalled

    monkeypatch.setattr(Spark, "__init__", capture_parent_init)

    with pytest.raises(_ParentInitCalled):
        engine_class(**constructor_kwargs, tblproperties=tblproperties)

    assert captured_kwargs["tblproperties"] is tblproperties
