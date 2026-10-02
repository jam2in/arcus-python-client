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


"""Stable public facade over composed, data-type-specific APIs."""

from .api import BTreeAPI, KeyValueAPI, ListAPI, RequestExecutor, SetAPI
from .collections import ArcusList, ArcusSet


class Arcus:
    def __init__(self, locator):
        self._executor = RequestExecutor(locator)
        self._kv = KeyValueAPI(self._executor)
        self._lists = ListAPI(self._executor)
        self._sets = SetAPI(self._executor)
        self._btree = BTreeAPI(self._executor)

    @property
    def locator(self):
        return self._executor.locator

    @locator.setter
    def locator(self, locator):
        self._executor.locator = locator

    def connect(self, addr, code):
        self.locator.connect(addr, code)

    def disconnect(self):
        self.locator.disconnect()

    def set(self, key, val, exptime=0):
        return self._kv.set(key, val, exptime)

    def get(self, key):
        return self._kv.get(key)

    def gets(self, key):
        return self._kv.gets(key)

    def incr(self, key, val=1):
        return self._kv.incr(key, val)

    def decr(self, key, val=1):
        return self._kv.decr(key, val)

    def delete(self, key):
        return self._kv.delete(key)

    def add(self, key, val, exptime=0):
        return self._kv.add(key, val, exptime)

    def append(self, key, val, exptime=0):
        return self._kv.append(key, val, exptime)

    def prepend(self, key, val, exptime=0):
        return self._kv.prepend(key, val, exptime)

    def replace(self, key, val, exptime=0):
        return self._kv.replace(key, val, exptime)

    def cas(self, key, val, cas_id, exptime=0):
        return self._kv.cas(key, val, cas_id, exptime)

    def lop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._lists.create(key, flags, exptime, noreply, attr_map)

    def lop_insert(self, key, index, value, noreply=False, pipe=False, attr_map=None):
        return self._lists.insert(key, index, value, noreply, pipe, attr_map)

    def lop_get(self, key, range, delete=False, drop=False):
        return self._lists.get(key, range, delete, drop)

    def lop_delete(self, key, range, drop=False, noreply=False, pipe=False):
        return self._lists.delete(key, range, drop, noreply, pipe)

    def sop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._sets.create(key, flags, exptime, noreply, attr_map)

    def sop_insert(self, key, value, noreply=False, pipe=False, attr_map=None):
        return self._sets.insert(key, value, noreply, pipe, attr_map)

    def sop_get(self, key, count=0, delete=False, drop=False):
        return self._sets.get(key, count, delete, drop)

    def sop_delete(self, key, value, drop=False, noreply=False, pipe=False):
        return self._sets.delete(key, value, drop, noreply, pipe)

    def sop_exist(self, key, value, pipe=False):
        return self._sets.exist(key, value, pipe)

    def bop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._btree.create(key, flags, exptime, noreply, attr_map)

    def bop_insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._btree.insert(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._btree.upsert(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._btree.update(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_get(self, key, range, filter=None, delete=False, drop=False):
        return self._btree.get(key, range, filter, delete, drop)

    def bop_delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        return self._btree.delete(key, range, filter, count, drop, noreply, pipe)

    def bop_count(self, key, range, filter=None):
        return self._btree.count(key, range, filter)

    def bop_incr(self, key, bkey, value, noreply=False, pipe=False):
        return self._btree.incr(key, bkey, value, noreply, pipe)

    def bop_decr(self, key, bkey, value, noreply=False, pipe=False):
        return self._btree.decr(key, bkey, value, noreply, pipe)

    def bop_mget(self, key_list, range, filter=None, offset=None, count=50):
        return self._btree.mget(key_list, range, filter, offset, count)

    def bop_smget(self, key_list, range, filter=None, offset=None, count=2000):
        return self._btree.smget(key_list, range, filter, offset, count)

    def list_alloc(self, key, flags, exptime=0, cache_time=0):
        self.lop_create(key, flags, exptime)
        return self.list_get(key, cache_time)

    def list_get(self, key, cache_time=0):
        return ArcusList(self, key, cache_time)

    def set_alloc(self, key, flags, exptime=0, cache_time=0):
        self.sop_create(key, flags, exptime)
        return self.set_get(key, cache_time)

    def set_get(self, key, cache_time=0):
        return ArcusSet(self, key, cache_time)
