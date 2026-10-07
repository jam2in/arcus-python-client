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


"""Public client and collection exceptions."""


class ArcusException(Exception):
    def __init__(self, msg):
        self.msg = msg


class ArcusProtocolException(ArcusException):
    def __init__(self, msg):
        self.msg = msg


class ArcusNodeException(ArcusException):
    def __init__(self, msg):
        self.msg = msg


class ArcusNodeSocketException(ArcusNodeException):
    def __init__(self, msg):
        self.msg = msg


class ArcusNodeConnectionException(ArcusNodeException):
    def __init__(self, msg):
        self.msg = msg


class ArcusListException(ArcusException):
    def __init__(self, msg):
        self.msg = msg


class CollectionException(ArcusException):
    def __init__(self, msg):
        self.msg = msg


class CollectionType(CollectionException):
    def __init__(self, msg="collection type mismatch"):
        self.msg = msg


class CollectionExist(CollectionException):
    def __init__(self, msg="collection already exits"):
        self.msg = msg


class CollectionIndex(CollectionException):
    def __init__(self, msg="invalid index or range"):
        self.msg = msg


class CollectionOverflow(CollectionException):
    def __init__(self, msg="collection overflow"):
        self.msg = msg


class CollectionUnreadable(CollectionException):
    def __init__(self, msg="collection is unreadable"):
        self.msg = msg


class CollectionHexFormat(CollectionException):
    def __init__(self, msg="invalid hex string format"):
        self.msg = msg


class FilterInvalid(CollectionException):
    def __init__(self, msg="invalid fiter expression"):
        self.msg = msg
