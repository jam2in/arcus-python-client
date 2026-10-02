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


"""Node transport, operation lifetime and compatibility command delegates."""

import socket
import threading
import time
from threading import Lock

from .._logging import arcuslog
from ..exceptions import (
    ArcusNodeConnectionException,
    ArcusNodeSocketException,
    ArcusProtocolException,
)
from ..operation import ArcusOperation
from .commands import CommandHandlers
from .connection import Connection
from .responses import ResponseHandlers


class ArcusMCNode:
    worker = None
    shutdown = False

    def __init__(self, addr, name, transcoder, node_allocator):
        self.addr = addr
        self.name = name
        self.in_use = False
        self.transcoder = transcoder
        self.node_allocator = node_allocator
        self.handle = Connection(
            addr,
            connect_timeout=getattr(node_allocator, "connect_timeout", 1.0),
            io_timeout=getattr(node_allocator, "io_timeout", 1.0),
        )
        self.ops = []
        self.lock = Lock()
        self._io_lock = threading.RLock()
        self._generation = 0
        self._pending = set()
        self._closed = False
        self.responses = ResponseHandlers(self.handle, self.transcoder)
        self.commands = CommandHandlers(self, self.transcoder, self.responses)

    def __repr__(self):
        return "%s-%s" % (self.addr, self.name)

    def get_fileno(self):
        return self.handle.socket.fileno()

    def disconnect(self):
        with self._io_lock:
            first_error = None
            for cleanup in (
                lambda: self.node_allocator.worker.poll.unregister_node(self),
                self.handle.disconnect,
            ):
                try:
                    cleanup()
                except Exception as error:
                    if first_error is None:
                        first_error = error
            with self.lock:
                self._generation += 1
                ops = list(self._pending | set(self.ops))
                self._pending.clear()
                self.ops = []
            for op in ops:
                op.set_invalid()
            if first_error is not None:
                raise first_error

    def _fail_operation(self, op, error):
        with self.lock:
            self._pending.discard(op)
            if op in self.ops:
                self.ops.remove(op)
        try:
            self.disconnect()
        except Exception as cleanup_error:
            arcuslog(self, "operation cleanup failed: %s" % str(cleanup_error))
        finally:
            op.set_result(error)

    def close(self):
        with self._io_lock:
            self._closed = True
            self.disconnect()
        self.node_allocator.forget(self)

    def disconnect_all(self):
        self.node_allocator.close()

    def process_request(self, request):
        if self.handle.disconnected():
            ret = self.handle.connect()
            if ret is not None:
                self.node_allocator.worker.register_node(self)
        self.handle.send_request(request)

    def process_operation(self, op):
        with self._io_lock:
            if (
                self._closed
                or self.node_allocator.shutdown
                or op.invalid
                or (op.generation != self._generation)
            ):
                op.set_invalid()
                return
            try:
                if time.monotonic() >= op.deadline:
                    raise socket.timeout("operation deadline exceeded before send")
                self.handle.deadline = op.deadline
                self.process_request(op.request)
            except Exception as error:
                self._fail_operation(op, error)
                return
            finally:
                self.handle.deadline = None
            if op.noreply:
                with self.lock:
                    self._pending.discard(op)
                op.set_result(True)

    def expire_operations(self):
        with self._io_lock:
            with self.lock:
                expired = any((op.deadline <= time.monotonic() for op in self._pending))
            if expired:
                with self.lock:
                    ops = list(self._pending)
                    self._pending.clear()
                    self.ops = []
                try:
                    self.disconnect()
                finally:
                    for op in ops:
                        op.set_result(socket.timeout("operation deadline exceeded"))

    def add_op(self, cmd, full_cmd, callback, noreply=False):
        op = ArcusOperation(self, full_cmd, callback)
        arcuslog(
            self,
            "add operation %s(%s:%s) to %s" % (full_cmd, callback, hex(id(op)), self),
        )
        with self._io_lock:
            with self.lock:
                if self._closed or self.node_allocator.shutdown:
                    op.set_invalid()
                    return op
                op.generation = self._generation
                op.deadline = time.monotonic() + getattr(
                    self.node_allocator, "operation_timeout", 5.0
                )
                op.noreply = noreply
                self._pending.add(op)
                if not noreply:
                    self.ops.append(op)
                self.node_allocator.worker.q.put(op)
        return op

    def do_op(self):
        with self._io_lock:
            while True:
                with self.lock:
                    op = self.ops.pop(0) if self.ops else None
                if op is None:
                    self.disconnect()
                    return
                try:
                    self.handle.deadline = op.deadline
                    self.responses.reader.begin()
                    ret = op.callback()
                except (
                    ArcusNodeConnectionException,
                    ArcusNodeSocketException,
                    ArcusProtocolException,
                    OSError,
                ) as error:
                    self._fail_operation(op, error)
                    return
                except Exception as error:
                    arcuslog(self, "do op failed: %s" % str(error))
                    if not self.responses.reader.complete:
                        self._fail_operation(op, error)
                        return
                    ret = error
                finally:
                    self.handle.deadline = None
                with self.lock:
                    self._pending.discard(op)
                op.set_result(ret)
                if not self.handle.hasline():
                    return

    def submit(self, request):
        """Queue an immutable command; lifecycle and completion stay with this node."""
        return self.add_op(
            request.name, request.payload, request.response, request.noreply
        )

    def get(self, key):
        return self.commands.kv.get(key)

    def gets(self, key):
        return self.commands.kv.gets(key)

    def set(self, key, val, exptime=0):
        return self.commands.kv.set(key, val, exptime)

    def cas(self, key, val, cas_id, exptime=0):
        return self.commands.kv.cas(key, val, cas_id, exptime)

    def incr(self, key, value=1):
        return self.commands.kv.incr(key, value)

    def decr(self, key, value=1):
        return self.commands.kv.decr(key, value)

    def add(self, key, val, exptime=0):
        return self.commands.kv.add(key, val, exptime)

    def append(self, key, val, exptime=0):
        return self.commands.kv.append(key, val, exptime)

    def prepend(self, key, val, exptime=0):
        return self.commands.kv.prepend(key, val, exptime)

    def replace(self, key, val, exptime=0):
        return self.commands.kv.replace(key, val, exptime)

    def delete(self, key):
        return self.commands.kv.delete(key)

    def flush_all(self):
        return self.commands.admin.flush_all()

    def get_stats(self, stat_args=None):
        return self.commands.admin.get_stats(stat_args)

    def lop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.list.create(key, flags, exptime, noreply, attr)

    def lop_insert(self, key, index, value, noreply=False, pipe=False, attr=None):
        return self.commands.list.insert(key, index, value, noreply, pipe, attr)

    def lop_delete(self, key, range, drop=False, noreply=False, pipe=False):
        return self.commands.list.delete(key, range, drop, noreply, pipe)

    def lop_get(self, key, range, delete=False, drop=False):
        return self.commands.list.get(key, range, delete, drop)

    def sop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.set.create(key, flags, exptime, noreply, attr)

    def sop_insert(self, key, value, noreply=False, pipe=False, attr=None):
        return self.commands.set.insert(key, value, noreply, pipe, attr)

    def sop_get(self, key, count=0, delete=False, drop=False):
        return self.commands.set.get(key, count, delete, drop)

    def sop_delete(self, key, val, drop=False, noreply=False, pipe=False):
        return self.commands.set.delete(key, val, drop, noreply, pipe)

    def sop_exist(self, key, val, pipe=False):
        return self.commands.set.exist(key, val, pipe)

    def bop_create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self.commands.btree.create(key, flags, exptime, noreply, attr)

    def bop_insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.insert(key, bkey, value, eflag, noreply, pipe, attr)

    def bop_upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.upsert(key, bkey, value, eflag, noreply, pipe, attr)

    def bop_update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self.commands.btree.update(key, bkey, value, eflag, noreply, pipe, attr)

    def bop_delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        return self.commands.btree.delete(
            key, range, filter, count, drop, noreply, pipe
        )

    def bop_get(self, key, range, filter=None, delete=False, drop=False):
        return self.commands.btree.get(key, range, filter, delete, drop)

    def bop_mget(self, key_list, range, filter=None, offset=None, count=50):
        return self.commands.btree.mget(key_list, range, filter, offset, count)

    def bop_smget(self, key_list, range, filter=None, offset=None, count=2000):
        return self.commands.btree.smget(key_list, range, filter, offset, count)

    def bop_count(self, key, range, filter):
        return self.commands.btree.count(key, range, filter)

    def bop_incr(self, key, bkey, value, noreply=False, pipe=False):
        return self.commands.btree.incr(key, bkey, value, noreply, pipe)

    def bop_decr(self, key, bkey, value, noreply=False, pipe=False):
        return self.commands.btree.decr(key, bkey, value, noreply, pipe)
