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


"""Set operations and their routing requirements."""

from ..collections import ArcusSet


class SetAPI:
    def __init__(self, executor, client=None):
        self._executor = executor
        self._client = client

    def create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._executor.execute(
            key,
            lambda node: node.commands.set.create(
                key, flags, exptime, noreply, attr_map
            ),
        )

    def insert(self, key, value, noreply=False, pipe=False, attr_map=None):
        return self._executor.execute(
            key,
            lambda node: node.commands.set.insert(key, value, noreply, pipe, attr_map),
        )

    def get(self, key, count=0, delete=False, drop=False):
        return self._executor.execute(
            key, lambda node: node.commands.set.get(key, count, delete, drop)
        )

    def delete(self, key, value, drop=False, noreply=False, pipe=False):
        return self._executor.execute(
            key, lambda node: node.commands.set.delete(key, value, drop, noreply, pipe)
        )

    def exist(self, key, value, pipe=False):
        return self._executor.execute(
            key, lambda node: node.commands.set.exist(key, value, pipe)
        )

    def alloc(self, key, flags, exptime=0, cache_time=0):
        """Create a collection and return its Python wrapper."""
        self.create(key, flags, exptime)
        return self.wrap(key, cache_time)

    def wrap(self, key, cache_time=0):
        """Return a Python wrapper for an existing collection key."""
        return ArcusSet(self._client, key, cache_time, _api=self)
