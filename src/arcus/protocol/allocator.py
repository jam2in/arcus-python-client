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


"""Cache-node construction and worker lifecycle."""

import queue
import threading

from ..exceptions import ArcusNodeConnectionException
from .node import ArcusMCNode
from .worker import ArcusMCWorker


class ArcusMCNodeAllocator:
    def __init__(self, transcoder):
        self.transcoder = transcoder
        self.worker = ArcusMCWorker(self)
        self.worker.start()
        self.shutdown = False

    def alloc(self, addr, name):
        ret = ArcusMCNode(addr, name, self.transcoder, self)
        self.worker.register_node(ret)
        return ret

    def join(self):
        self.worker.join()
