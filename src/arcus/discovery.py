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


"""ZooKeeper membership discovery, independent of cache routing and allocation."""

import time
from threading import Lock

from kazoo.client import KazooClient, KazooState
from kazoo.protocol.states import EventType

from ._logging import arcuslog


# Match Kazoo's default, sharing one budget across connection and initial reads.
ZOOKEEPER_CONNECT_TIMEOUT = 15.0


def _remaining_connect_timeout(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("ZooKeeper connection setup deadline exceeded")
    return remaining


class ZooKeeperDiscovery:
    """Publish ordered membership snapshots to a supplied callback.

    The callback accepts a list of child names, without knowing about ZooKeeper.
    Publication is serialized with session invalidation. It must not call back
    into this object's lifecycle methods. No ZooKeeper wait holds this lock.
    """

    def __init__(self, on_membership):
        self._on_membership = on_membership
        self._lock = Lock()
        self._lifecycle_lock = Lock()
        self._client = None
        self._path = None
        self._generation = 0
        self._refresh_requested = 0
        self._refresh_applied = 0
        self._watch = None

    @property
    def client(self):
        return self._client

    @property
    def path(self):
        return self._path

    def connect(self, addr, code):
        with self._lifecycle_lock:
            if self._client is not None:
                self._close()
            try:
                zk = KazooClient(hosts=addr)
                with self._lock:
                    self._client = zk
                    self._path = "/arcus/cache_list/" + code
                    self._generation += 1
                    generation = self._generation
                    self._watch = lambda event: self._watch_children(
                        zk, generation, event
                    )
                    path, watch = self._path, self._watch
                zk.add_listener(
                    lambda state: self._state_changed(zk, generation, state)
                )
                deadline = time.monotonic() + ZOOKEEPER_CONNECT_TIMEOUT
                zk.start(timeout=_remaining_connect_timeout(deadline))
                data, stat = zk.get_async(path).get(
                    timeout=_remaining_connect_timeout(deadline)
                )
                arcuslog(self, "zoo keeper node info with stat: ", data, stat)
                with self._lock:
                    self._refresh_requested += 1
                    request_id = self._refresh_requested
                children = zk.get_children_async(path, watch=watch).get(
                    timeout=_remaining_connect_timeout(deadline)
                )
                self._publish(zk, generation, request_id, children)
            except BaseException:
                try:
                    self._close()
                except Exception as error:
                    arcuslog(self, "discovery cleanup failed: ", error)
                raise

    def close(self):
        with self._lifecycle_lock:
            self._close()

    def _close(self):
        with self._lock:
            self._generation += 1
            zk, self._client = self._client, None
        if zk is None:
            return
        # A stop failure must not leak Kazoo's sockets or handler threads.
        first_error = None
        for close in (zk.stop, zk.close):
            try:
                close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def watch_children(self, event):
        """Handle a watch notification for the currently connected session."""
        with self._lock:
            zk, generation = self._client, self._generation
        self._watch_children(zk, generation, event)

    def _watch_children(self, zk, generation, event):
        if getattr(event, "type", None) == EventType.NONE:
            # Kazoo clears one-shot watches when suspended. CONNECTED refreshes
            # membership and reinstalls the watch without a blocking read.
            return
        self._refresh_children(zk, generation)

    def _state_changed(self, zk, generation, state):
        if state == KazooState.CONNECTED:
            self._refresh_children(zk, generation)

    def _refresh_children(self, zk, generation):
        with self._lock:
            if zk is None or self._client is not zk or self._generation != generation:
                return
            self._refresh_requested += 1
            request_id = self._refresh_requested
            path, watch = self._path, self._watch
        # State listeners run on Kazoo's connection thread, so only its async
        # API is safe here. Completion callbacks run on Kazoo's handler.
        try:
            result = zk.get_children_async(path, watch=watch)
            result.rawlink(
                lambda result: self._refresh_complete(
                    zk, generation, request_id, result
                )
            )
        except Exception as error:
            arcuslog(self, "discovery refresh failed: ", error)

    def _refresh_complete(self, zk, generation, request_id, result):
        try:
            self._publish(zk, generation, request_id, result.get())
        except Exception as error:
            arcuslog(self, "discovery refresh failed: ", error)

    def _publish(self, zk, generation, request_id, children):
        with self._lock:
            if (
                self._client is not zk
                or self._generation != generation
                or request_id <= self._refresh_applied
            ):
                return
            self._on_membership(children)
            self._refresh_applied = request_id
