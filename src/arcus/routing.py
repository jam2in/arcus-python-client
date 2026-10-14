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


class ArcusReplicaGroup:
    def __init__(self, group_name):
        self.group_name = group_name
        self.master_node = None
        self.slave_node = None
        self.in_use = False

    def get_master(self):
        return self.master_node

    def get_slave(self):
        return self.slave_node if self.slave_node else self.master_node

    def __repr__(self):
        return "<ArcusReplicaGroup[%s] M=%s S=%s>" % (
            self.group_name,
            self.master_node.addr if self.master_node else None,
            self.slave_node.addr if self.slave_node else None,
        )


class ArcusLocator:
    def __init__(self, node_allocator):
        self.zk = None
        self.hash_method = ArcusKetemaHash()

        self.lock = Lock()
        self.node_list = []
        self.addr_node_map = {}
        self.group_map = {}
        self.node_allocator = node_allocator

    def connect(self, addr, code):
        # init zookeeper
        arcuslog(self, "zoo keeper init")
        self.zk = KazooClient(hosts=addr)
        self.zk.start()

        # an enterprise (replication) cluster registers under /arcus_repl
        zoo_path = "/arcus_repl/cache_list/" + code
        if not self.zk.exists(zoo_path):
            zoo_path = "/arcus/cache_list/" + code
        arcuslog(self, "zoo keeper get path: " + zoo_path)
        data, stat = self.zk.get(zoo_path)
        arcuslog(self, "zoo keeper node info with stat: ", data, stat)

        children = self.zk.get_children(path=zoo_path, watch=self.watch_children)
        self.hash_nodes(children)

    def disconnect(self):
        for node in self.addr_node_map.values():
            node.disconnect_all()

        self.addr_node_map = {}
        self.group_map = {}
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
        for group in self.group_map.values():
            group.in_use = False

        # update live nodes
        group_children = {}
        for child in children:
            if "^" in child:
                # enterprise: <group>^<M|S>^<ip:port>-<hostname>, applied per group below
                group_name, role, child = child.split("^", 2)
                addr, _, name = child.partition("-")
                group_children.setdefault(group_name, []).append((role, addr, name))
                continue

            lst = child.split("-")
            addr, name = lst[:2]

            node = self.__use_node(addr, name)

            hash_list = self.hash_method.hash(node.addr)
            arcuslog(self, "hash_lists of node(%s): %s" % (node.addr, hash_list))

            for hash in hash_list:
                point = ArcusPoint(hash, node)
                self.node_list.append(point)

        for group_name, members in group_children.items():
            group = self.group_map.get(group_name)
            if not self.__valid_group(group_name, members):
                # failover/switchover in progress: keep the previous roles and nodes
                # until the cluster settles, like the java client
                if group:
                    group.in_use = True
                    for node in (group.master_node, group.slave_node):
                        if node:
                            node.in_use = True
                continue

            if group is None:
                group = ArcusReplicaGroup(group_name)
                self.group_map[group_name] = group

            group.in_use = True
            group.master_node = None
            group.slave_node = None
            for role, addr, name in members:
                node = self.__use_node(addr, name)
                if role == "M":
                    group.master_node = node
                else:
                    group.slave_node = node

        # remove dead groups, then hash live groups by group name
        for group_name in [g for g, grp in self.group_map.items() if not grp.in_use]:
            del self.group_map[group_name]

        for group_name, group in self.group_map.items():
            hash_list = self.hash_method.hash(group_name)
            arcuslog(self, "hash_lists of group(%s): %s" % (group_name, hash_list))

            for hash in hash_list:
                point = ArcusPoint(hash, group)
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

    def __use_node(self, addr, name):
        if addr in self.addr_node_map:
            node = self.addr_node_map[addr]
        else:
            # new node
            node = self.node_allocator.alloc(addr, name)
            self.addr_node_map[addr] = node

        node.in_use = True
        return node

    def __valid_group(self, group_name, members):
        # one master, at most one slave and no duplicated address
        masters = [member for member in members if member[0] == "M"]
        addrs = set(member[1] for member in members)
        if len(masters) == 1 and len(members) <= 2 and len(addrs) == len(members):
            return True

        arcuslog(self, "invalid group(%s): %s" % (group_name, members))
        return False

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

        node = point.node
        if isinstance(node, ArcusReplicaGroup):
            return node.get_master()

        return node

    def __hash_key(self, key):
        bkey = bytes(key, "utf-8")

        m = hashlib.md5()
        m.update(bkey)
        r = m.digest()
        ret = r[3] << 24 | r[2] << 16 | r[1] << 8 | r[0]
        return ret
