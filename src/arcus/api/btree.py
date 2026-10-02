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


"""BTree operations and their routing requirements."""

from ..operation import ArcusOperationList


class BTreeAPI:
    def __init__(self, executor):
        self._executor = executor

    def create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._executor.execute(
            key, lambda node: node.bop_create(key, flags, exptime, noreply, attr_map)
        )

    def insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._executor.execute(
            key,
            lambda node: node.bop_insert(
                key, bkey, value, eflag, noreply, pipe, attr_map
            ),
        )

    def upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._executor.execute(
            key,
            lambda node: node.bop_upsert(
                key, bkey, value, eflag, noreply, pipe, attr_map
            ),
        )

    def update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr_map=None
    ):
        return self._executor.execute(
            key,
            lambda node: node.bop_update(
                key, bkey, value, eflag, noreply, pipe, attr_map
            ),
        )

    def get(self, key, range, filter=None, delete=False, drop=False):
        return self._executor.execute(
            key, lambda node: node.bop_get(key, range, filter, delete, drop)
        )

    def delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        return self._executor.execute(
            key,
            lambda node: node.bop_delete(
                key, range, filter, count, drop, noreply, pipe
            ),
        )

    def count(self, key, range, filter=None):
        return self._executor.execute(
            key, lambda node: node.bop_count(key, range, filter)
        )

    def incr(self, key, bkey, value, noreply=False, pipe=False):
        return self._executor.execute(
            key, lambda node: node.bop_incr(key, bkey, value, noreply, pipe)
        )

    def decr(self, key, bkey, value, noreply=False, pipe=False):
        return self._executor.execute(
            key, lambda node: node.bop_decr(key, bkey, value, noreply, pipe)
        )

    def mget(self, key_list, range, filter=None, offset=None, count=50):
        operations = ArcusOperationList("bop mget")
        for node, keys in self._executor.group_by_node(key_list).items():
            operations.add_op(node.bop_mget(keys, range, filter, offset, count))
        return operations

    def smget(self, key_list, range, filter=None, offset=None, count=2000):
        operations = ArcusOperationList("bop smget")
        for node, keys in self._executor.group_by_node(key_list).items():
            operations.add_op(node.bop_smget(keys, range, filter, offset, count))
        return operations
