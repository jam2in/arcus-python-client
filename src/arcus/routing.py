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


"""Consistent hashing and ZooKeeper cache-node discovery."""

import bisect
import hashlib
from threading import Lock

from kazoo.client import KazooClient

from ._logging import arcuslog
from .exceptions import ArcusNodeConnectionException, ArcusProtocolException


class ArcusKetemaHash:
    def __init__(self):
        # config
        self.per_node = 40
        self.per_hash = 4

    def hash(self, addr):
        ret = []
        for i in range(0, self.per_node):
            ret = ret + self.__hash(addr + ("-%d" % i))

        return ret

    def __hash(self, input):
        m = hashlib.md5()
        m.update(bytes(input, "utf-8"))
        r = m.digest()
        ret = []

        for i in range(0, self.per_hash):
            hash = (
                (r[3 + i * 4] << 24)
                | (r[2 + i * 4] << 16)
                | (r[1 + i * 4] << 8)
                | r[0 + i * 4]
            )
            ret.append(hash)

        return ret


class ArcusPoint:
    def __init__(self, hash, node):
        self.hash = hash
        self.node = node

    def __lt__(self, rhs):
        return self.hash < rhs.hash

    def __le__(self, rhs):
        return self.hash <= rhs.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __ne__(self, rhs):
        return self.hash != rhs.hash

    def __gt__(self, rhs):
        return self.hash > rhs.hash

    def __ge__(self, rhs):
        return self.hash >= rhs.hash

    def __repr__(self):
        return "(%d:%s)" % (self.hash, self.node)


class ArcusLocator:
    def __init__(self, node_allocator):
        # config
        self.hash_method = ArcusKetemaHash()

        # init
        self.lock = Lock()
        self.node_list = []
        self.addr_node_map = {}
        self.node_allocator = node_allocator

    def connect(self, addr, code):
        # init zookeeper
        arcuslog(self, "zoo keeper init")
        self.zk = KazooClient(hosts=addr)
        self.zk.start()

        self.zoo_path = "/arcus/cache_list/" + code
        arcuslog(self, "zoo keeper get path: " + self.zoo_path)
        data, stat = self.zk.get(self.zoo_path)
        arcuslog(self, "zoo keeper node info with stat: ", data, stat)

        children = self.zk.get_children(self.zoo_path, watch=self.watch_children)
        self.hash_nodes(children)

    def disconnect(self):
        for node in self.addr_node_map.values():
            node.disconnect_all()

        self.addr_node_map = {}
        self.node_list = []
        self.zk.stop()
        self.node_allocator.join()

    def hash_nodes(self, children):
        # print ('hash_nodes with children %d' % len(children))
        arcuslog(self, "hash_nodes with children: ", children)

        self.lock.acquire()

        # clear first
        self.node_list = []
        for node in self.addr_node_map.values():
            node.in_use = False

        # update live nodes
        for child in children:
            lst = child.split("-")
            addr, name = lst[:2]

            if addr in self.addr_node_map:
                self.addr_node_map[addr].in_use = True
                node = self.addr_node_map[addr]
            else:
                # new node
                node = self.node_allocator.alloc(addr, name)
                self.addr_node_map[addr] = node
                self.addr_node_map[addr].in_use = True

            hash_list = self.hash_method.hash(node.addr)
            arcuslog(self, "hash_lists of node(%s): %s" % (node.addr, hash_list))

            for hash in hash_list:
                point = ArcusPoint(hash, node)
                self.node_list.append(point)

        # sort list
        self.node_list.sort()
        arcuslog(self, "sorted node list", self.node_list)

        # disconnect dead node
        dead_list = []
        for addr, node in self.addr_node_map.items():
            if node.in_use == False:
                dead_list.append(node)

        for node in dead_list:
            arcuslog(self, "disconnect node(%s)" % node.addr)
            node.disconnect()
            del self.addr_node_map[node.addr]

        self.lock.release()

    def watch_children(self, event):
        arcuslog(self, "watch children called: ", event)

        # rehashing
        children = self.zk.get_children(event.path, watch=self.watch_children)
        self.hash_nodes(children)

    def get_node(self, key):
        hash = self.__hash_key(key)

        self.lock.acquire()
        idx = bisect.bisect(self.node_list, ArcusPoint(hash, None))

        # roll over
        if idx >= len(self.node_list):
            idx = 0

        point = self.node_list[idx]
        self.lock.release()

        return point.node

    def __hash_key(self, key):
        bkey = bytes(key, "utf-8")

        m = hashlib.md5()
        m.update(bkey)
        r = m.digest()
        ret = r[3] << 24 | r[2] << 16 | r[1] << 8 | r[0]
        return ret
