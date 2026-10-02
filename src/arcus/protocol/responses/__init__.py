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


"""Compose response handlers over one narrow byte reader."""

from .admin import AdminResponses
from .btree import BTreeResponses
from .collection import CollectionResponses
from .kv import KVResponses
from .list import ListResponses
from .reader import ResponseReader
from .set import SetResponses
from .status import StatusResponses


class ResponseHandlers:
    def __init__(self, connection, transcoder):
        self.reader = ResponseReader(connection, transcoder)
        self.status = StatusResponses(self.reader)
        self.collection = CollectionResponses(self.reader)
        self.kv = KVResponses(self.reader)
        self.list = ListResponses(self.reader, self.collection)
        self.set = SetResponses(self.reader, self.collection)
        self.btree = BTreeResponses(self.reader, self.collection)
        self.admin = AdminResponses(self.reader)


__all__ = [
    "AdminResponses",
    "BTreeResponses",
    "CollectionResponses",
    "KVResponses",
    "ListResponses",
    "ResponseHandlers",
    "ResponseReader",
    "SetResponses",
    "StatusResponses",
]
