"""Compatibility imports for applications using the former flat module."""

import warnings

from arcus.protocol import (
    ArcusMCNodeAllocator,
    Connection,
    EflagFilter,
    ArcusMCNode,
    ArcusMCPoll,
    ArcusMCWorker,
)

__deprecated__ = (
    "arcus_mc_node is deprecated; import these names from arcus.protocol instead."
)
warnings.warn(__deprecated__, DeprecationWarning, stacklevel=2)

__all__ = [
    "ArcusMCNodeAllocator",
    "Connection",
    "EflagFilter",
    "ArcusMCNode",
    "ArcusMCPoll",
    "ArcusMCWorker",
]
