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


"""Public cache API and request routing facade."""

import time

from .collections import ArcusList, ArcusSet
from .operation import ArcusOperationList


class Arcus:
    def __init__(self, locator):
        self.locator = locator

    def connect(self, addr, code):
        self.locator.connect(addr, code)

    def disconnect(self):
        self.locator.disconnect()

    def set(self, key, val, exptime=0):
        node = self.locator.get_node(key)
        return node.set(key, val, exptime)

    def get(self, key):
        node = self.locator.get_node(key)
        return node.get(key)

    def gets(self, key):
        node = self.locator.get_node(key)
        return node.gets(key)

    def incr(self, key, val=1):
        node = self.locator.get_node(key)
        return node.incr(key, val)

    def decr(self, key, val=1):
        node = self.locator.get_node(key)
        return node.decr(key, val)

    def delete(self, key):
        node = self.locator.get_node(key)
        return node.delete(key)

    def add(self, key, val, exptime=0):
        node = self.locator.get_node(key)
        return node.add(key, val, exptime)

    def append(self, key, val, exptime=0):
        node = self.locator.get_node(key)
        return node.append(key, val, exptime)

    def prepend(self, key, val, exptime=0):
        node = self.locator.get_node(key)
        return node.prepend(key, val, exptime)

    def replace(self, key, val, exptime=0):
        node = self.locator.get_node(key)
        return node.replace(key, val, exptime)

    def cas(self, key, val, cas_id, exptime=0):
        node = self.locator.get_node(key)
        return node.cas(key, val, cas_id, time)

    def lop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        node = self.locator.get_node(key)
        return node.lop_create(key, flags, exptime, noreply, attr_map)

    def lop_insert(self, key, index, value, noreply=False, pipe=False, attr_map=None):
        node = self.locator.get_node(key)
        return node.lop_insert(key, index, value, noreply, pipe, attr_map)

    def lop_get(self, key, range, delete=False, drop=False):
        node = self.locator.get_node(key)
        return node.lop_get(key, range, delete, drop)

    def lop_delete(self, key, range, drop=False, noreply=False, pipe=False):
        node = self.locator.get_node(key)
        return node.lop_delete(key, range, drop, noreply, pipe)

    def sop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        node = self.locator.get_node(key)
        return node.sop_create(key, flags, exptime, noreply, attr_map)

    def sop_insert(self, key, value, noreply=False, pipe=False, attr_map=None):
        node = self.locator.get_node(key)
        return node.sop_insert(key, value, noreply, pipe, attr_map)

    def sop_get(self, key, count=0, delete=False, drop=False):
        node = self.locator.get_node(key)
        return node.sop_get(key, count, delete, drop)

    def sop_delete(self, key, value, drop=False, noreply=False, pipe=False):
        node = self.locator.get_node(key)
        return node.sop_delete(key, value, drop, noreply, pipe)

    def sop_exist(self, key, value, pipe=False):
        node = self.locator.get_node(key)
        return node.sop_exist(key, value, pipe)

    def bop_create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        node = self.locator.get_node(key)
        return node.bop_create(key, flags, exptime, noreply, attr_map)

    def bop_insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        node = self.locator.get_node(key)
        return node.bop_insert(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        node = self.locator.get_node(key)
        return node.bop_upsert(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        node = self.locator.get_node(key)
        return node.bop_update(key, bkey, value, eflag, noreply, pipe, attr_map)

    def bop_get(self, key, range, filter=None, delete=False, drop=False):
        node = self.locator.get_node(key)
        return node.bop_get(key, range, filter, delete, drop)

    def bop_delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        node = self.locator.get_node(key)
        return node.bop_delete(key, range, filter, count, drop, noreply, pipe)

    def bop_count(self, key, range, filter=None):
        node = self.locator.get_node(key)
        return node.bop_count(key, range, filter)

    def bop_incr(self, key, bkey, value, noreply=False, pipe=False):
        node = self.locator.get_node(key)
        return node.bop_incr(key, bkey, value, noreply, pipe)

    def bop_decr(self, key, bkey, value, noreply=False, pipe=False):
        node = self.locator.get_node(key)
        return node.bop_incr(key, bkey, value, noreply, pipe)

    def bop_mget(self, key_list, range, filter=None, offset=None, count=50):
        nodes = {}

        for key in key_list:
            node = self.locator.get_node(key)
            if node not in nodes:
                nodes[node] = [key]
            else:
                nodes[node].append(key)

        op_list = ArcusOperationList("bop mget")
        for node in nodes:
            op = node.bop_mget(nodes[node], range, filter, offset, count)
            op_list.add_op(op)

        return op_list

    def bop_smget(self, key_list, range, filter=None, offset=None, count=2000):
        nodes = {}

        for key in key_list:
            node = self.locator.get_node(key)
            if node not in nodes:
                nodes[node] = [key]
            else:
                nodes[node].append(key)

        op_list = ArcusOperationList("bop smget")
        for node in nodes:
            op = node.bop_smget(nodes[node], range, filter, offset, count)
            op_list.add_op(op)

        return op_list

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
