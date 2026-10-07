from .base import BaseEngine


class DeltaRs(BaseEngine):
    """Provide shared Delta Lake writes for non-Spark engines.

    Notes
    -----
    This is an internal engine helper rather than a standalone benchmark
    execution engine. Non-Spark engines delegate Delta table persistence to
    this class. It exposes the delta-rs ``write_deltalake`` function and
    ``DeltaTable`` class after construction.
    """

    def __init__(self):
        """
        Initialize the Delta-rs Engine Configs
        """
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake

        self.write_deltalake = write_deltalake
        self.DeltaTable = DeltaTable
