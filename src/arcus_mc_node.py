"""Compatibility imports for applications using the former flat module."""

# The former module started with ``from arcus import *``, so it also exposed the
# public ``arcus`` API. Keep that for existing imports from this module.
from arcus import *
from arcus import __all__ as _arcus_all
from arcus.protocol import (
    ArcusMCNodeAllocator,
    Connection,
    EflagFilter,
    ArcusMCNode,
    ArcusMCPoll,
    ArcusMCWorker,
)

__all__ = [
    "ArcusMCNodeAllocator",
    "Connection",
    "EflagFilter",
    "ArcusMCNode",
    "ArcusMCPoll",
    "ArcusMCWorker",
]
__all__ += [name for name in _arcus_all if name not in __all__]
