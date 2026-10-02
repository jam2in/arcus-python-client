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


"""Request dispatch and Linux epoll response processing."""

import errno
import queue
import select
import threading
from threading import Lock

from .._logging import arcuslog


class ArcusMCPoll(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.epoll = select.epoll()
        self.sock_node_map = {}
        self.node_allocator = node_allocator

    def run(self):
        arcuslog(self, "epoll start")

        while True:
            events = self.epoll.poll(2)

            if self.node_allocator.shutdown == True:
                arcuslog(self, "epoll out")
                return

            for fileno, event in events:
                if event & select.EPOLLIN:
                    node = self.sock_node_map[fileno]
                    node.do_op()

                if event & select.EPOLLHUP:
                    print("EPOLL HUP")
                    self.epoll.unregister(fileno)
                    node = self.sock_node_map[fileno]
                    node.disconnect()
                    del self.sock_node_map[fileno]

    def register_node(self, node):
        self.epoll.register(node.get_fileno(), select.EPOLLIN | select.EPOLLHUP)

        arcuslog(self, "regist node: ", node.get_fileno(), node)
        self.sock_node_map[node.get_fileno()] = node


class ArcusMCWorker(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.q = queue.Queue()
        self.poll = ArcusMCPoll(node_allocator)
        self.poll.start()
        self.node_allocator = node_allocator

    def run(self):
        arcuslog(self, "worker start")

        while True:
            op = self.q.get()
            if self.node_allocator.shutdown == True:
                arcuslog(self, "worker done")
                self.poll.join()
                return

            if op == None:  # maybe shutdown
                continue

            arcuslog(
                self,
                "get operation %s(%s:%s) from %s"
                % (op.request, op.callback, hex(id(op)), op.node),
            )
            node = op.node

            try:
                node.process_request(op.request)
            except Exception as e:
                arcuslog(self, "operation failed: %s" % str(e))
                op.set_result(e)

    def register_node(self, node):
        self.poll.register_node(node)
