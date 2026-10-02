"""Compose data-type command objects over a single submission capability."""

from ..request import CommandSubmitter
from .admin import AdminCommands
from .btree import BTreeCommands
from .collection import CollectionCommandBuilder
from .kv import KVCommands
from .list import ListCommands
from .set import SetCommands


class CommandHandlers:
    def __init__(self, executor: CommandSubmitter, transcoder, responses):
        collection = CollectionCommandBuilder(transcoder, responses.collection)
        self.kv = KVCommands(executor, transcoder, responses)
        self.list = ListCommands(executor, collection, responses.list, responses.status)
        self.set = SetCommands(
            executor, transcoder, collection, responses.set, responses.status
        )
        self.btree = BTreeCommands(
            executor, collection, responses.btree, responses.status
        )
        self.admin = AdminCommands(executor, responses.admin)


__all__ = [
    "AdminCommands",
    "BTreeCommands",
    "CollectionCommandBuilder",
    "CommandHandlers",
    "KVCommands",
    "ListCommands",
    "SetCommands",
]
