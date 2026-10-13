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
    def __init__(self, msg="Arcus client error"):
        super().__init__(msg)


class ArcusCompositeException(ArcusException):
    def __init__(self, exceptions):
        self.exceptions = tuple(exceptions)
        messages = ", ".join(str(e) or type(e).__name__ for e in self.exceptions)
        super().__init__(
            f"Multiple exceptions ({len(self.exceptions)}) reported: {messages}"
        )


class ArcusProtocolException(ArcusException):
    def __init__(self, msg="Arcus protocol error"):
        super().__init__(msg)


class ArcusNodeException(ArcusException):
    def __init__(self, msg="Arcus node error"):
        super().__init__(msg)


class ArcusNodeSocketException(ArcusNodeException):
    def __init__(self, msg="Arcus node socket error"):
        super().__init__(msg)


class ArcusNodeConnectionException(ArcusNodeException):
    def __init__(self, msg="Node disconnected"):
        super().__init__(msg)


class ArcusListException(ArcusException):
    def __init__(self, msg="Arcus list operation failed"):
        super().__init__(msg)


class ArcusTimeoutException(ArcusException):
    def __init__(self, msg="Timed out waiting for the operation result"):
        super().__init__(msg)


class CollectionException(ArcusException):
    def __init__(self, msg="Collection operation failed"):
        super().__init__(msg)


class CollectionType(CollectionException):
    def __init__(self, msg="Collection type mismatch"):
        super().__init__(msg)


class CollectionExist(CollectionException):
    def __init__(self, msg="Collection already exists"):
        super().__init__(msg)


class CollectionIndex(CollectionException):
    def __init__(self, msg="Invalid index or range"):
        super().__init__(msg)


class CollectionOverflow(CollectionException):
    def __init__(self, msg="Collection overflow"):
        super().__init__(msg)


class CollectionUnreadable(CollectionException):
    def __init__(self, msg="Collection is unreadable"):
        super().__init__(msg)


class CollectionHexFormat(CollectionException):
    def __init__(self, msg="Invalid hex string format"):
        super().__init__(msg)


class FilterInvalid(CollectionException):
    def __init__(self, msg="Invalid filter expression"):
        super().__init__(msg)
