"""LakeBench: multi-engine lakehouse benchmarking library."""

import logging as _logging

# Library convention: attach a NullHandler so importing lakebench does not
# emit log records to stderr unless the consumer (or the CLI) configures
# logging. The CLI sets up `logging.basicConfig` itself in `_configure_logging`.
_logging.getLogger(__name__).addHandler(_logging.NullHandler())
