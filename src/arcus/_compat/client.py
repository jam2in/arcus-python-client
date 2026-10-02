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


"""Deprecated flat client methods retained with their original signatures."""

from .._deprecation import deprecated


class LegacyArcusAPI:
    @deprecated("Arcus.set() is deprecated; use Arcus.kv.set() instead.")
    def set(self, key, val, exptime=0):
        return self.kv.set(key, val, exptime)

    @deprecated("Arcus.get() is deprecated; use Arcus.kv.get() instead.")
    def get(self, key):
        return self.kv.get(key)

    @deprecated("Arcus.gets() is deprecated; use Arcus.kv.gets() instead.")
    def gets(self, key):
        return self.kv.gets(key)

    @deprecated("Arcus.incr() is deprecated; use Arcus.kv.incr() instead.")
    def incr(self, key, val=1):
        return self.kv.incr(key, val)

    @deprecated("Arcus.decr() is deprecated; use Arcus.kv.decr() instead.")
    def decr(self, key, val=1):
        return self.kv.decr(key, val)

    @deprecated("Arcus.delete() is deprecated; use Arcus.kv.delete() instead.")
    def delete(self, key):
        return self.kv.delete(key)

    @deprecated("Arcus.add() is deprecated; use Arcus.kv.add() instead.")
    def add(self, key, val, exptime=0):
        return self.kv.add(key, val, exptime)

    @deprecated("Arcus.append() is deprecated; use Arcus.kv.append() instead.")
    def append(self, key, val, exptime=0):
        return self.kv.append(key, val, exptime)

    @deprecated("Arcus.prepend() is deprecated; use Arcus.kv.prepend() instead.")
    def prepend(self, key, val, exptime=0):
        return self.kv.prepend(key, val, exptime)

    @deprecated("Arcus.replace() is deprecated; use Arcus.kv.replace() instead.")
    def replace(self, key, val, exptime=0):
        return self.kv.replace(key, val, exptime)

    @deprecated("Arcus.cas() is deprecated; use Arcus.kv.cas() instead.")
    def cas(self, key, val, cas_id, exptime=0):
        return self.kv.cas(key, val, cas_id, exptime)

    @deprecated("Arcus.lop_create() is deprecated; use Arcus.lop.create() instead.")
    def lop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self.lop.create(key, flags, exptime, noreply, attr_map)

    @deprecated("Arcus.lop_insert() is deprecated; use Arcus.lop.insert() instead.")
    def lop_insert(self, key, index, value, noreply=False, pipe=False, attr_map=None):
        return self.lop.insert(key, index, value, noreply, pipe, attr_map)

    @deprecated("Arcus.lop_get() is deprecated; use Arcus.lop.get() instead.")
    def lop_get(self, key, range, delete=False, drop=False):
        return self.lop.get(key, range, delete, drop)

    @deprecated("Arcus.lop_delete() is deprecated; use Arcus.lop.delete() instead.")
    def lop_delete(self, key, range, drop=False, noreply=False, pipe=False):
        return self.lop.delete(key, range, drop, noreply, pipe)

    @deprecated("Arcus.sop_create() is deprecated; use Arcus.sop.create() instead.")
    def sop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self.sop.create(key, flags, exptime, noreply, attr_map)

    @deprecated("Arcus.sop_insert() is deprecated; use Arcus.sop.insert() instead.")
    def sop_insert(self, key, value, noreply=False, pipe=False, attr_map=None):
        return self.sop.insert(key, value, noreply, pipe, attr_map)

    @deprecated("Arcus.sop_get() is deprecated; use Arcus.sop.get() instead.")
    def sop_get(self, key, count=0, delete=False, drop=False):
        return self.sop.get(key, count, delete, drop)

    @deprecated("Arcus.sop_delete() is deprecated; use Arcus.sop.delete() instead.")
    def sop_delete(self, key, value, drop=False, noreply=False, pipe=False):
        return self.sop.delete(key, value, drop, noreply, pipe)

    @deprecated("Arcus.sop_exist() is deprecated; use Arcus.sop.exist() instead.")
    def sop_exist(self, key, value, pipe=False):
        return self.sop.exist(key, value, pipe)

    @deprecated("Arcus.bop_create() is deprecated; use Arcus.bop.create() instead.")
    def bop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self.bop.create(key, flags, exptime, noreply, attr_map)

    @deprecated("Arcus.bop_insert() is deprecated; use Arcus.bop.insert() instead.")
    def bop_insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self.bop.insert(key, bkey, value, eflag, noreply, pipe, attr_map)

    @deprecated("Arcus.bop_upsert() is deprecated; use Arcus.bop.upsert() instead.")
    def bop_upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self.bop.upsert(key, bkey, value, eflag, noreply, pipe, attr_map)

    @deprecated("Arcus.bop_update() is deprecated; use Arcus.bop.update() instead.")
    def bop_update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self.bop.update(key, bkey, value, eflag, noreply, pipe, attr_map)

    @deprecated("Arcus.bop_get() is deprecated; use Arcus.bop.get() instead.")
    def bop_get(self, key, range, filter=None, delete=False, drop=False):
        return self.bop.get(key, range, filter, delete, drop)

    @deprecated("Arcus.bop_delete() is deprecated; use Arcus.bop.delete() instead.")
    def bop_delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        return self.bop.delete(key, range, filter, count, drop, noreply, pipe)

    @deprecated("Arcus.bop_count() is deprecated; use Arcus.bop.count() instead.")
    def bop_count(self, key, range, filter=None):
        return self.bop.count(key, range, filter)

    @deprecated("Arcus.bop_incr() is deprecated; use Arcus.bop.incr() instead.")
    def bop_incr(self, key, bkey, value, noreply=False, pipe=False):
        return self.bop.incr(key, bkey, value, noreply, pipe)

    @deprecated("Arcus.bop_decr() is deprecated; use Arcus.bop.decr() instead.")
    def bop_decr(self, key, bkey, value, noreply=False, pipe=False):
        return self.bop.decr(key, bkey, value, noreply, pipe)

    @deprecated("Arcus.bop_mget() is deprecated; use Arcus.bop.mget() instead.")
    def bop_mget(self, key_list, range, filter=None, offset=None, count=50):
        return self.bop.mget(key_list, range, filter, offset, count)

    @deprecated("Arcus.bop_smget() is deprecated; use Arcus.bop.smget() instead.")
    def bop_smget(self, key_list, range, filter=None, offset=None, count=2000):
        return self.bop.smget(key_list, range, filter, offset, count)

    @deprecated("Arcus.list_alloc() is deprecated; use Arcus.lop.alloc() instead.")
    def list_alloc(self, key, flags, exptime=0, cache_time=0):
        return self.lop.alloc(key, flags, exptime, cache_time)

    @deprecated("Arcus.list_get() is deprecated; use Arcus.lop.wrap() instead.")
    def list_get(self, key, cache_time=0):
        return self.lop.wrap(key, cache_time)

    @deprecated("Arcus.set_alloc() is deprecated; use Arcus.sop.alloc() instead.")
    def set_alloc(self, key, flags, exptime=0, cache_time=0):
        return self.sop.alloc(key, flags, exptime, cache_time)

    @deprecated("Arcus.set_get() is deprecated; use Arcus.sop.wrap() instead.")
    def set_get(self, key, cache_time=0):
        return self.sop.wrap(key, cache_time)
