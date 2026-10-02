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


"""List operations and their routing requirements."""


class ListAPI:
    def __init__(self, executor):
        self._executor = executor

    def create(self, key, flags, exptime=0, noreply=False, attr_map=None):
        return self._executor.execute(
            key, lambda node: node.lop_create(key, flags, exptime, noreply, attr_map)
        )

    def insert(self, key, index, value, noreply=False, pipe=False, attr_map=None):
        return self._executor.execute(
            key,
            lambda node: node.lop_insert(key, index, value, noreply, pipe, attr_map),
        )

    def get(self, key, range, delete=False, drop=False):
        return self._executor.execute(
            key, lambda node: node.lop_get(key, range, delete, drop)
        )

    def delete(self, key, range, drop=False, noreply=False, pipe=False):
        return self._executor.execute(
            key, lambda node: node.lop_delete(key, range, drop, noreply, pipe)
        )
