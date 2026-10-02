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


"""Python collection wrappers around the existing Arcus APIs."""

import time

from .exceptions import ArcusListException


class ArcusList:
    def __init__(self, arcus, key, cache_time=0):
        self.arcus = arcus
        self.key = key
        self.cache_time = cache_time

        if cache_time > 0:
            try:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
            except Exception:
                self.cache = []
        else:
            self.cache = None

        self.next_refresh = time.time() + cache_time

    def __len__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return len(self.cache)
        else:
            return len(self.arcus.lop_get(self.key, (0, -1)).get_result())

    def __iter__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return iter(self.cache)
        else:
            return iter(self.arcus.lop_get(self.key, (0, -1)).get_result())

    def __eq__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache == rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() == rhs

    def __ne__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache != rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() != rhs

    def __le__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache <= rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() <= rhs

    def __lt__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache < rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() < rhs

    def __ge__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache >= rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() >= rhs

    def __gt__(self, rhs):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache > rhs
        else:
            return self.arcus.lop_get(self.key, (0, -1)).get_result() > rhs

    def __getitem__(self, index):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return self.cache[index]
        else:
            if isinstance(index, slice):
                start = index.start
                stop = index.stop
                if stop != None:
                    stop -= 1

                if start == None:
                    start = 0
                if stop == None:
                    stop = -1

                try:
                    return self.arcus.lop_get(self.key, (start, stop)).get_result()
                except Exception:
                    return []
            else:
                ret = self.arcus.lop_get(self.key, index).get_result()
                if len(ret) == 0:
                    raise IndexError("lop index out of range")

                return ret[0]

    def __setitem__(self, index, value):
        raise ArcusListException("list set is not possible")

    def __delitem__(self, index):
        if self.cache != None:
            del self.cache[index]

        if isinstance(index, slice):
            start = index.start
            stop = index.stop
            if stop != None:
                stop -= 1

            if start == None:
                start = 0
            if stop == None:
                stop = -1
            return self.arcus.lop_delete(self.key, (start, stop)).get_result()
        else:
            return self.arcus.lop_delete(self.key, index).get_result()

    def insert(self, index, value):
        if self.cache != None:
            self.cache.insert(index, value)

        return self.arcus.lop_insert(self.key, index, value).get_result()

    def append(self, value):
        if self.cache != None:
            self.cache.append(value)

        return self.arcus.lop_insert(self.key, -1, value).get_result()

    def invalidate(self):
        if self.cache != None:
            try:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
            except Exception:
                self.cache = []

            self.next_refresh = time.time() + self.cache_time

    def __repr__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.lop_get(self.key, (0, -1)).get_result()
                self.next_refresh = time.time() + self.cache_time
            return repr(self.cache)

        try:
            ret = self.arcus.lop_get(self.key, (0, -1)).get_result()
        except Exception:
            ret = []  # not found?

        return repr(ret)


class ArcusSet:
    def __init__(self, arcus, key, cache_time=0):
        self.arcus = arcus
        self.key = key
        self.cache_time = cache_time

        if cache_time > 0:
            try:
                self.cache = self.arcus.sop_get(self.key).get_result()
            except Exception:
                self.cache = set()
        else:
            self.cache = None

        self.next_refresh = time.time() + cache_time

    def __len__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.sop_get(self.key).get_result()
                self.next_refresh = time.time() + self.cache_time
            return len(self.cache)
        else:
            return len(self.arcus.sop_get(self.key).get_result())

    def __contains__(self, value):
        if self.cache != None and time.time() < self.next_refresh:
            return value in self.cache  # do not fetch all for cache when time over

        return self.arcus.sop_exist(self.key, value).get_result()

    def __iter__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.sop_get(self.key).get_result()
                self.next_refresh = time.time() + self.cache_time
            return iter(self.cache)
        else:
            return iter(self.arcus.sop_get(self.key).get_result())

    def add(self, value):
        result = self.arcus.sop_insert(self.key, value).get_result()
        if result is True and self.cache is not None:
            self.cache.add(value)
        return result

    def invalidate(self):
        if self.cache != None:
            try:
                self.cache = self.arcus.sop_get(self.key).get_result()
            except Exception:
                self.cache = set()

            self.next_refresh = time.time() + self.cache_time

    def __repr__(self):
        if self.cache != None:
            if time.time() >= self.next_refresh:
                self.cache = self.arcus.sop_get(self.key).get_result()
                self.next_refresh = time.time() + self.cache_time
            return repr(self.cache)

        try:
            ret = self.arcus.sop_get(self.key).get_result()
        except Exception:
            ret = set()

        return repr(ret)
