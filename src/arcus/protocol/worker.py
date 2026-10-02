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


class ArcusMCPoll(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.epoll = select.epoll()
        self.sock_node_map = {}
        self.node_allocator = node_allocator
        self.lock = Lock()

    def run(self):
        try:
            while not self.node_allocator.shutdown:
                events = self.epoll.poll(0.05)
                for fileno, event in events:
                    with self.lock:
                        node = self.sock_node_map.get(fileno)
                    if node is None:
                        continue
                    if event & select.EPOLLIN:
                        node.do_op()
                    if event & (select.EPOLLHUP | select.EPOLLERR):
                        node.disconnect()
                for node in self.node_allocator.snapshot_nodes():
                    node.expire_operations()
        finally:
            self.epoll.close()

    def unregister_node(self, node):
        with self.lock:
            descriptors = [
                fd for fd, existing in self.sock_node_map.items() if existing is node
            ]
            for fd in descriptors:
                self.sock_node_map.pop(fd, None)
                try:
                    if not self.epoll.closed:
                        self.epoll.unregister(fd)
                except OSError as error:
                    if error.errno not in (errno.EBADF, errno.ENOENT):
                        raise

    def register_node(self, node):
        self.unregister_node(node)
        with self.lock:
            if self.node_allocator.shutdown or node.handle.disconnected():
                return
            fileno = node.get_fileno()
            self.epoll.register(
                fileno, select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR
            )
            self.sock_node_map[fileno] = node


class ArcusMCWorker(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.q = queue.Queue()
        self.node_allocator = node_allocator
        self.poll = ArcusMCPoll(node_allocator)
        self.poll.start()

    def run(self):
        try:
            while True:
                op = self.q.get()
                if self.node_allocator.shutdown:
                    return
                if op is None or op.invalid:
                    continue
                op.node.process_operation(op)
        finally:
            self.poll.join()

    def register_node(self, node):
        self.poll.register_node(node)
