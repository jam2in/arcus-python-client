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


"""KeyValue operations and their routing requirements."""


class KeyValueAPI:
    def __init__(self, executor):
        self._executor = executor

    def set(self, key, val, exptime=0):
        return self._executor.execute(key, lambda node: node.set(key, val, exptime))

    def get(self, key):
        return self._executor.execute(key, lambda node: node.get(key))

    def gets(self, key):
        return self._executor.execute(key, lambda node: node.gets(key))

    def incr(self, key, val=1):
        return self._executor.execute(key, lambda node: node.incr(key, val))

    def decr(self, key, val=1):
        return self._executor.execute(key, lambda node: node.decr(key, val))

    def delete(self, key):
        return self._executor.execute(key, lambda node: node.delete(key))

    def add(self, key, val, exptime=0):
        return self._executor.execute(key, lambda node: node.add(key, val, exptime))

    def append(self, key, val, exptime=0):
        return self._executor.execute(key, lambda node: node.append(key, val, exptime))

    def prepend(self, key, val, exptime=0):
        return self._executor.execute(key, lambda node: node.prepend(key, val, exptime))

    def replace(self, key, val, exptime=0):
        return self._executor.execute(key, lambda node: node.replace(key, val, exptime))

    def cas(self, key, val, cas_id, exptime=0):
        return self._executor.execute(
            key, lambda node: node.cas(key, val, cas_id, exptime)
        )
