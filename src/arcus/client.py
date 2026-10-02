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


"""Public client composing explicit data-type API namespaces."""

from ._compat.client import LegacyArcusAPI
from .api import BTreeAPI, KeyValueAPI, ListAPI, RequestExecutor, SetAPI


class Arcus(LegacyArcusAPI):
    def __init__(self, locator):
        self._executor = RequestExecutor(locator)
        self.kv = KeyValueAPI(self._executor)
        self.lop = ListAPI(self._executor, client=self)
        self.sop = SetAPI(self._executor, client=self)
        self.bop = BTreeAPI(self._executor)

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
