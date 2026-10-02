#
# arcus-python-client - Arcus python client drvier
# Copyright 2014 NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Deprecated node entry points delegated to command collaborators."""

from .._deprecation import deprecated
from ..protocol.request import CommandRequest


class LegacyNodeCommands:
    """Retain old node calls while directing callers to composed commands."""

    @deprecated(
        "ArcusMCNode.disconnect_all() is deprecated; use node.node_allocator.close() instead."
    )
    def disconnect_all(self):
        self.node_allocator.close()

    @deprecated(
        "ArcusMCNode.add_op() is deprecated; use node.submit(CommandRequest(...)) instead."
    )
    def add_op(self, cmd, full_cmd, callback, noreply=False):
        return self.submit(CommandRequest(cmd, full_cmd, callback, noreply))

    @deprecated("ArcusMCNode.get() is deprecated; use node.commands.kv.get() instead.")
    def get(self, key):
        return self.commands.kv.get(key)

    @deprecated(
        "ArcusMCNode.gets() is deprecated; use node.commands.kv.gets() instead."
    )
    def gets(self, key):
        return self.commands.kv.gets(key)

    @deprecated("ArcusMCNode.set() is deprecated; use node.commands.kv.set() instead.")
    def set(self, key, val, exptime=0):
        return self.commands.kv.set(key, val, exptime)

    @deprecated("ArcusMCNode.cas() is deprecated; use node.commands.kv.cas() instead.")
    def cas(self, key, val, cas_id, exptime=0):
        return self.commands.kv.cas(key, val, cas_id, exptime)

    @deprecated(
        "ArcusMCNode.incr() is deprecated; use node.commands.kv.incr() instead."
    )
    def incr(self, key, value=1):
        return self.commands.kv.incr(key, value)

    @deprecated(
        "ArcusMCNode.decr() is deprecated; use node.commands.kv.decr() instead."
    )
    def decr(self, key, value=1):
        return self.commands.kv.decr(key, value)

    @deprecated("ArcusMCNode.add() is deprecated; use node.commands.kv.add() instead.")
    def add(self, key, val, exptime=0):
        return self.commands.kv.add(key, val, exptime)

    @deprecated(
        "ArcusMCNode.append() is deprecated; use node.commands.kv.append() instead."
    )
    def append(self, key, val, exptime=0):
        return self.commands.kv.append(key, val, exptime)

    @deprecated(
        "ArcusMCNode.prepend() is deprecated; use node.commands.kv.prepend() instead."
    )
    def prepend(self, key, val, exptime=0):
        return self.commands.kv.prepend(key, val, exptime)

    @deprecated(
        "ArcusMCNode.replace() is deprecated; use node.commands.kv.replace() instead."
    )
    def replace(self, key, val, exptime=0):
        return self.commands.kv.replace(key, val, exptime)

    @deprecated(
        "ArcusMCNode.delete() is deprecated; use node.commands.kv.delete() instead."
    )
    def delete(self, key):
        return self.commands.kv.delete(key)

    @deprecated(
        "ArcusMCNode.flush_all() is deprecated; use node.commands.admin.flush_all() instead."
    )
    def flush_all(self):
        return self.commands.admin.flush_all()

    @deprecated(
        "ArcusMCNode.get_stats() is deprecated; use node.commands.admin.get_stats() instead."
    )
    def get_stats(self, stat_args=None):
        return self.commands.admin.get_stats(stat_args)

    @deprecated(
        "ArcusMCNode.lop_create() is deprecated; use node.commands.list.create() instead."
    )
    def lop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.list.create(key, flags, exptime, noreply, attr)

    @deprecated(
        "ArcusMCNode.lop_insert() is deprecated; use node.commands.list.insert() instead."
    )
    def lop_insert(self, key, index, value, noreply=False, pipe=False, attr=None):
        return self.commands.list.insert(key, index, value, noreply, pipe, attr)

    @deprecated(
        "ArcusMCNode.lop_delete() is deprecated; use node.commands.list.delete() instead."
    )
    def lop_delete(self, key, range, drop=False, noreply=False, pipe=False):
        return self.commands.list.delete(key, range, drop, noreply, pipe)

    @deprecated(
        "ArcusMCNode.lop_get() is deprecated; use node.commands.list.get() instead."
    )
    def lop_get(self, key, range, delete=False, drop=False):
        return self.commands.list.get(key, range, delete, drop)

    @deprecated(
        "ArcusMCNode.sop_create() is deprecated; use node.commands.set.create() instead."
    )
    def sop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.set.create(key, flags, exptime, noreply, attr)

    @deprecated(
        "ArcusMCNode.sop_insert() is deprecated; use node.commands.set.insert() instead."
    )
    def sop_insert(self, key, value, noreply=False, pipe=False, attr=None):
        return self.commands.set.insert(key, value, noreply, pipe, attr)

    @deprecated(
        "ArcusMCNode.sop_get() is deprecated; use node.commands.set.get() instead."
    )
    def sop_get(self, key, count=0, delete=False, drop=False):
        return self.commands.set.get(key, count, delete, drop)

    @deprecated(
        "ArcusMCNode.sop_delete() is deprecated; use node.commands.set.delete() instead."
    )
    def sop_delete(self, key, val, drop=False, noreply=False, pipe=False):
        return self.commands.set.delete(key, val, drop, noreply, pipe)

    @deprecated(
        "ArcusMCNode.sop_exist() is deprecated; use node.commands.set.exist() instead."
    )
    def sop_exist(self, key, val, pipe=False):
        return self.commands.set.exist(key, val, pipe)

    @deprecated(
        "ArcusMCNode.bop_create() is deprecated; use node.commands.btree.create() instead."
    )
    def bop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.btree.create(key, flags, exptime, noreply, attr)

    @deprecated(
        "ArcusMCNode.bop_insert() is deprecated; use node.commands.btree.insert() instead."
    )
    def bop_insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.insert(key, bkey, value, eflag, noreply, pipe, attr)

    @deprecated(
        "ArcusMCNode.bop_upsert() is deprecated; use node.commands.btree.upsert() instead."
    )
    def bop_upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.upsert(key, bkey, value, eflag, noreply, pipe, attr)

    @deprecated(
        "ArcusMCNode.bop_update() is deprecated; use node.commands.btree.update() instead."
    )
    def bop_update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.update(key, bkey, value, eflag, noreply, pipe, attr)

    @deprecated(
        "ArcusMCNode.bop_delete() is deprecated; use node.commands.btree.delete() instead."
    )
    def bop_delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        return self.commands.btree.delete(
            key, range, filter, count, drop, noreply, pipe
        )

    @deprecated(
        "ArcusMCNode.bop_get() is deprecated; use node.commands.btree.get() instead."
    )
    def bop_get(self, key, range, filter=None, delete=False, drop=False):
        return self.commands.btree.get(key, range, filter, delete, drop)

    @deprecated(
        "ArcusMCNode.bop_mget() is deprecated; use node.commands.btree.mget() instead."
    )
    def bop_mget(self, key_list, range, filter=None, offset=None, count=50):
        return self.commands.btree.mget(key_list, range, filter, offset, count)

    @deprecated(
        "ArcusMCNode.bop_smget() is deprecated; use node.commands.btree.smget() instead."
    )
    def bop_smget(self, key_list, range, filter=None, offset=None, count=2000):
        return self.commands.btree.smget(key_list, range, filter, offset, count)

    @deprecated(
        "ArcusMCNode.bop_count() is deprecated; use node.commands.btree.count() instead."
    )
    def bop_count(self, key, range, filter):
        return self.commands.btree.count(key, range, filter)

    @deprecated(
        "ArcusMCNode.bop_incr() is deprecated; use node.commands.btree.incr() instead."
    )
    def bop_incr(self, key, bkey, value, noreply=False, pipe=False):
        return self.commands.btree.incr(key, bkey, value, noreply, pipe)

    @deprecated(
        "ArcusMCNode.bop_decr() is deprecated; use node.commands.btree.decr() instead."
    )
    def bop_decr(self, key, bkey, value, noreply=False, pipe=False):
        return self.commands.btree.decr(key, bkey, value, noreply, pipe)
