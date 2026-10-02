"""Bounded Linux fault experiments against a disposable Arcus service.

TCP proxies inject faults only into this runner's connections. They do not need
host networking privileges or access to the Docker socket.
"""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
from pathlib import Path
import queue
import select
import socket
import threading
import time
import traceback
from uuid import uuid4

from kazoo.client import KazooClient
from arcus import Arcus, ArcusException, ArcusLocator, ArcusTranscoder
from arcus_mc_node import ArcusMCNodeAllocator

RESULTS = Path("/results")
PREFIX = "stability:" + uuid4().hex
TTL = 300
CONNECT_TIMEOUT = 0.5
IO_TIMEOUT = 0.5
OPERATION_TIMEOUT = 2.0
WAIT_TIMEOUT = 4.0
LOG_LOCK = threading.Lock()
EVENTS = (RESULTS / "events.jsonl").open("w", buffering=1)
REQUESTS = (RESULTS / "requests.jsonl").open("w", buffering=1)
METRICS = (RESULTS / "metrics.jsonl").open("w", buffering=1)


def record(kind, **fields):
    entry = {"time": time.time(), "monotonic": time.monotonic(), "kind": kind, **fields}
    with LOG_LOCK:
        EVENTS.write(json.dumps(entry) + "\n")
    print(json.dumps(entry), flush=True)


def read_control(path):
    # Desktop bind mounts can briefly expose a rename between separate calls.
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def wait_until(predicate, timeout=20, description="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


class Proxy:
    def __init__(self, target):
        host, port = target.rsplit(":", 1)
        self.target = (host, int(port))
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen()
        self.server.settimeout(0.2)
        self.address = self.server.getsockname()
        self.hosts = f"{self.address[0]}:{self.address[1]}"
        self.mode = "normal"
        self.delay = 0.0
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()
        self.threads = []
        self.acceptor = threading.Thread(
            target=self.accept, name="fault-proxy", daemon=True
        )
        self.acceptor.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                incoming, _ = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if self.mode == "blocked":
                incoming.close()
                continue
            try:
                outgoing = socket.create_connection(self.target, timeout=1)
                outgoing.settimeout(None)
            except OSError:
                incoming.close()
                continue
            with self.lock:
                self.connections.update([incoming, outgoing])
            thread = threading.Thread(
                target=self.relay,
                args=(incoming, outgoing),
                name="fault-relay",
                daemon=True,
            )
            self.threads.append(thread)
            thread.start()

    def relay(self, incoming, outgoing):
        try:
            while not self.stop.is_set():
                readable, _, _ = select.select([incoming, outgoing], [], [], 0.1)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    if source is outgoing:
                        if self.mode == "silent":
                            continue
                        if self.mode == "delayed" and self.stop.wait(self.delay):
                            return
                    target = outgoing if source is incoming else incoming
                    target.sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            with self.lock:
                self.connections.discard(incoming)
                self.connections.discard(outgoing)
            incoming.close()
            outgoing.close()

    def configure(self, mode, delay=0.0):
        self.delay = delay
        self.mode = mode
        if mode in {"blocked", "drop"}:
            with self.lock:
                connections = list(self.connections)
            for connection in connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            if mode == "drop":
                self.mode = "normal"
        record("proxy", target=str(self.target), mode=mode, delay=delay)

    def close(self):
        self.stop.set()
        self.configure("blocked")
        self.server.close()
        self.acceptor.join(timeout=2)
        for thread in self.threads:
            thread.join(timeout=2)
        assert not self.acceptor.is_alive()
        assert not any(thread.is_alive() for thread in self.threads)


class Harness:
    def __init__(self):
        self.proxies = {}
        self.hosts = os.environ["ARCUS_TEST_ZOOKEEPER"]
        self.code = os.environ["ARCUS_TEST_SERVICE_CODE"]
        self.path = "/arcus/cache_list/" + self.code
        self.admin = KazooClient(hosts=self.hosts)
        self.admin.start(timeout=10)
        wait_until(
            lambda: len(self.admin.get_children(self.path)) == 2,
            description="two cache nodes",
        )
        self.zk_proxy = Proxy(self.hosts)
        self.allocator = self.make_allocator()
        self.locator = ArcusLocator(self.allocator)
        self.client = Arcus(self.locator)
        self.states = []
        self.client.connect(self.zk_proxy.hosts, self.code)
        self.locator.zk.add_listener(self.zk_state)
        self.fault_id = 0
        self.request_id = 0
        self.request_lock = threading.Lock()
        self.latencies = deque(maxlen=10000)
        self.errors = 0
        self.completed = 0
        self.baseline = self.metrics("connected")

    def make_allocator(self):
        allocator = ArcusMCNodeAllocator(
            ArcusTranscoder(),
            connect_timeout=CONNECT_TIMEOUT,
            io_timeout=IO_TIMEOUT,
            operation_timeout=OPERATION_TIMEOUT,
        )
        allocate = allocator.alloc

        def alloc(addr, name):
            proxy = self.proxies.get(addr)
            if proxy is None:
                proxy = self.proxies[addr] = Proxy(addr)
            node = allocate(proxy.hosts, name)
            # Keep logical addresses for routing; the physical connection uses
            # this runner's proxy. The allocator tracks nodes by identity.
            node.addr = addr
            return node

        allocator.alloc = alloc
        return allocator

    def zk_state(self, state):
        self.states.append(state)
        zk = self.locator.zk
        record(
            "zk-state",
            state=state,
            session=zk.client_id[0] if zk and zk.client_id else None,
        )

    def call(self, command, *args, expected=None, check=False, **kwargs):
        with self.request_lock:
            self.request_id += 1
            request_id = self.request_id
        start = time.monotonic()
        entry = {
            "id": request_id,
            "command": command,
            "key": str(args[0]),
            "submitted": start,
        }
        with LOG_LOCK:
            REQUESTS.write(json.dumps({**entry, "status": "submitted"}) + "\n")
        try:
            operation = getattr(self.client, command)(*args, **kwargs)
            entry["enqueued"] = time.monotonic()
            entry["node"] = getattr(getattr(operation, "node", None), "addr", None)
            value = operation.get_result(timeout=WAIT_TIMEOUT)
            entry["cache_miss"] = command == "get" and value is None
            if check:
                assert value == expected, (request_id, command, expected, value)
            entry["status"] = "completed"
            return value
        except Exception as error:
            entry["status"] = "error"
            entry["error"] = type(error).__name__
            raise
        finally:
            elapsed = time.monotonic() - start
            entry["completed"] = time.monotonic()
            entry["elapsed"] = elapsed
            with LOG_LOCK:
                REQUESTS.write(json.dumps(entry) + "\n")
                self.latencies.append(elapsed)
                self.completed += 1
                self.errors += entry["status"] != "completed"

    def round_trip(self, key, value="value"):
        self.call("set", key, value, exptime=TTL, expected=True, check=True)
        self.call("get", key, expected=value, check=True)

    def membership(self):
        with self.locator.lock:
            actual = sorted(self.locator.addr_node_map)
        expected = sorted(
            child.partition("-")[0] for child in self.admin.get_children(self.path)
        )
        record("membership", actual=actual, expected=expected)
        assert actual == expected
        return actual

    def metrics(self, label):
        with self.locator.lock:
            nodes = list(self.locator.addr_node_map.values())
        pending = 0
        for node in nodes:
            with node.lock:
                pending += len(node._pending)
        status = Path("/proc/self/status").read_text()
        rss = next(
            line.split()[1] for line in status.splitlines() if line.startswith("VmRSS:")
        )
        entry = {
            "time": time.time(),
            "label": label,
            "rss_kib": int(rss),
            "fds": len(os.listdir("/proc/self/fd")),
            "threads": threading.active_count(),
            "cpu_seconds": time.process_time(),
            "worker_queue": self.allocator.worker.q.qsize(),
            "pending_operations": pending,
            "requests": self.completed,
            "errors": self.errors,
        }
        with LOG_LOCK:
            METRICS.write(json.dumps(entry) + "\n")
        return entry

    def node_action(self, action, service):
        self.fault_id += 1
        request = {"id": self.fault_id, "action": action, "service": service}
        temporary = RESULTS / "fault-request.tmp"
        temporary.write_text(json.dumps(request))
        temporary.replace(RESULTS / "fault-request.json")
        record("node-fault-start", **request)
        wait_until(
            lambda: (
                read_control(RESULTS / "fault-done.json").get("id") == self.fault_id
            ),
            timeout=35,
            description=f"host action {action} {service}",
        )
        record("node-fault-done", **request)

    def expect_fault(self, function):
        start = time.monotonic()
        try:
            function()
        except (ArcusException, queue.Empty, OSError) as error:
            elapsed = time.monotonic() - start
            record("bounded-error", error=type(error).__name__, elapsed=elapsed)
            assert elapsed < WAIT_TIMEOUT + 0.5
            return
        raise AssertionError("Fault did not reach the caller as a defined error")

    def concurrent_clients(self):
        record("scenario-start", scenario="multiple-clients-and-collections")
        other_allocator = ArcusMCNodeAllocator(
            ArcusTranscoder(),
            connect_timeout=CONNECT_TIMEOUT,
            io_timeout=IO_TIMEOUT,
            operation_timeout=OPERATION_TIMEOUT,
        )
        other = Arcus(ArcusLocator(other_allocator))
        other.connect(self.hosts, self.code)
        try:

            def operations(worker):
                client = self.client if worker % 2 == 0 else other
                key = f"{PREFIX}:multiple:{worker}"
                for revision in range(12):
                    value = f"{worker}:{revision}"
                    assert client.set(key, value, exptime=TTL).get_result(WAIT_TIMEOUT)
                    assert client.get(key).get_result(WAIT_TIMEOUT) == value
                collection = key + ":list"
                assert client.lop_create(
                    collection, ArcusTranscoder.FLAG_STRING, exptime=TTL
                ).get_result(WAIT_TIMEOUT)
                expected = []
                for revision in range(6):
                    value = f"{worker}:{revision}"
                    expected.append(value)
                    assert client.lop_insert(collection, -1, value).get_result(
                        WAIT_TIMEOUT
                    )
                    assert (
                        client.lop_get(collection, (0, -1)).get_result(WAIT_TIMEOUT)
                        == expected
                    )
                record(
                    "multiple-client-worker",
                    worker=worker,
                    round_trips=12,
                    collection_round_trips=6,
                )

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(operations, worker) for worker in range(4)]
                for future in futures:
                    future.result(timeout=30)
        finally:
            other.disconnect()
            assert not other_allocator.worker.is_alive()
            assert not other_allocator.worker.poll.is_alive()
        self.round_trip(PREFIX + ":other-client-closed")
        record("scenario-pass", scenario="multiple-clients-and-collections")

    def network_faults(self):
        record("scenario-start", scenario="cache-network")
        key = PREFIX + ":network"
        self.round_trip(key)
        proxy = self.proxies[self.locator.get_node(key).addr]
        proxy.configure("delayed", delay=0.1)
        start = time.monotonic()
        self.call("get", key, expected="value", check=True)
        assert time.monotonic() - start >= 0.09
        proxy.configure("silent")
        # The server executes this increment, but its reply is deliberately lost.
        # Verify the client reports uncertainty and does not repeat the mutation.
        proxy.configure("normal")
        self.call("set", key, "1", exptime=TTL)
        proxy.configure("silent")
        self.expect_fault(lambda: self.call("incr", key, 1))
        proxy.configure("normal")
        self.call("get", key, expected="2", check=True)
        self.round_trip(key, "after-silent")
        proxy.configure("delayed", delay=3)
        self.expect_fault(lambda: self.call("get", key))
        proxy.configure("normal")
        self.round_trip(key, "after-late-reply")
        proxy.configure("drop")
        # Closing an idle socket may be noticed before submission or during it.
        try:
            self.call("get", key, expected="after-late-reply", check=True)
        except (ArcusException, queue.Empty, OSError):
            pass
        self.round_trip(key, "after-reset")
        record("scenario-pass", scenario="cache-network")

    def membership_faults(self):
        stop = threading.Event()
        started = threading.Event()
        failures = []
        outcomes = {"success": 0, "cache_miss": 0, "defined_error": 0}

        def traffic():
            revision = 0
            key = PREFIX + ":membership-traffic"
            while not stop.is_set():
                expected = f"membership:{revision}"
                try:
                    self.call(
                        "set", key, expected, exptime=TTL, expected=True, check=True
                    )
                    actual = self.call("get", key)
                    assert actual in (expected, None), (expected, actual)
                    outcomes["cache_miss" if actual is None else "success"] += 1
                except (ArcusException, queue.Empty, OSError):
                    outcomes["defined_error"] += 1
                except Exception as error:
                    failures.append(repr(error))
                    return
                started.set()
                revision += 1
                stop.wait(0.02)

        thread = threading.Thread(target=traffic, name="membership-traffic")
        thread.start()
        try:
            assert started.wait(WAIT_TIMEOUT + 1)
            self._membership_faults()
        finally:
            stop.set()
            thread.join(timeout=WAIT_TIMEOUT + 1)
        assert not thread.is_alive(), "Membership traffic did not terminate"
        assert not failures, failures
        record("membership-traffic-drained", **outcomes)

    def _membership_faults(self):
        record("scenario-start", scenario="membership-and-node-restart")
        self.membership()
        self.node_action("stop", "cache1")
        wait_until(
            lambda: len(self.locator.addr_node_map) == 1,
            timeout=30,
            description="cache1 removal",
        )
        self.membership()
        self.round_trip(PREFIX + ":survivor")
        self.node_action("start", "cache1")
        wait_until(
            lambda: len(self.locator.addr_node_map) == 2,
            timeout=30,
            description="cache1 recovery",
        )
        self.membership()
        self.round_trip(PREFIX + ":recovered")
        self.node_action("kill", "cache2")
        wait_until(
            lambda: len(self.locator.addr_node_map) == 1,
            timeout=40,
            description="killed cache2 session removal",
        )
        self.membership()
        self.node_action("start", "cache2")
        wait_until(
            lambda: len(self.locator.addr_node_map) == 2,
            timeout=30,
            description="cache2 recovery",
        )
        children = self.admin.get_children(self.path)
        entries = {
            child: self.admin.get(self.path + "/" + child)[0] for child in children
        }
        try:
            for child in children:
                self.admin.delete(self.path + "/" + child)
            wait_until(
                lambda: not self.locator.addr_node_map, description="empty membership"
            )
            self.expect_fault(lambda: self.client.get(PREFIX + ":empty"))
            self.membership()
        finally:
            for child, data in entries.items():
                if not self.admin.exists(self.path + "/" + child):
                    self.admin.create(self.path + "/" + child, data)
        wait_until(
            lambda: len(self.locator.addr_node_map) == 2,
            description="membership restoration",
        )
        self.membership()
        self.round_trip(PREFIX + ":after-empty")
        record("scenario-pass", scenario="membership-and-node-restart")

    def zookeeper_faults(self):
        record("scenario-start", scenario="zookeeper-connection-and-expiry")
        zk = self.locator.zk
        negotiated = zk._session_timeout / 1000
        first_session = zk.client_id[0]
        record("zk-session", session=first_session, negotiated_timeout=negotiated)
        self.zk_proxy.configure("blocked")
        wait_until(
            lambda: "SUSPENDED" in self.states,
            timeout=5,
            description="suspended ZooKeeper connection",
        )
        self.round_trip(PREFIX + ":zk-short")
        time.sleep(min(1, negotiated / 5))
        self.zk_proxy.configure("normal")
        wait_until(
            lambda: zk.connected, timeout=10, description="ZooKeeper reconnection"
        )
        assert zk.client_id[0] == first_session
        record("zk-short-recovery", session=zk.client_id[0])
        self.states.clear()
        self.zk_proxy.configure("blocked")
        # Remove one service entry through the independent admin connection while
        # the client is cut off from every configured ZooKeeper endpoint.
        child = self.admin.get_children(self.path)[0]
        path = self.path + "/" + child
        data = self.admin.get(path)[0]
        self.admin.delete(path)
        blocked_at = time.monotonic()
        try:
            time.sleep(negotiated + 5)
            restored_at = time.monotonic()
            self.zk_proxy.configure("normal")
            wait_until(
                lambda: zk.connected and zk.client_id[0] != first_session,
                timeout=20,
                description="a new ZooKeeper session",
            )
            wait_until(
                lambda: "LOST" in self.states,
                timeout=5,
                description="LOST session state",
            )
            record(
                "zk-expiry-evidence",
                old_session=first_session,
                new_session=zk.client_id[0],
                negotiated_timeout=negotiated,
                blocked_seconds=restored_at - blocked_at,
                recovery_seconds=time.monotonic() - restored_at,
                states=self.states[:],
            )
            wait_until(
                lambda: len(self.locator.addr_node_map) == 1,
                timeout=15,
                description="fresh membership after session expiry",
            )
            self.membership()
            self.round_trip(PREFIX + ":zk-expired")
        finally:
            self.zk_proxy.configure("normal")
            if not self.admin.exists(path):
                self.admin.create(path, data)
        wait_until(
            lambda: len(self.locator.addr_node_map) == 2,
            description="post-expiry watch re-registration",
        )
        self.membership()
        record("scenario-pass", scenario="zookeeper-connection-and-expiry")

    def concurrent_soak(self, duration):
        record(
            "scenario-start",
            scenario="shared-client-soak",
            duration=duration,
            workers=4,
            value_bytes=256,
            pause_seconds=0.02,
        )
        stop = threading.Event()
        failures = []

        def run(worker):
            revision = 0
            key = f"{PREFIX}:worker:{worker}"
            try:
                while not stop.is_set():
                    value = f"{worker}:{revision}:".ljust(256, "x")
                    self.round_trip(key, value)
                    revision += 1
                    stop.wait(0.02)
            except Exception as error:
                failures.append(repr(error))
                stop.set()

        before_errors = self.errors
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(run, worker) for worker in range(4)]
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline and not stop.is_set():
                self.metrics("soak")
                stop.wait(min(1, max(0, deadline - time.monotonic())))
            stop.set()
            for future in futures:
                future.result(timeout=WAIT_TIMEOUT + 1)
        assert not failures, failures
        assert self.errors == before_errors

        def queues_drained():
            if not self.allocator.worker.q.empty():
                return False
            with self.locator.lock:
                nodes = list(self.locator.addr_node_map.values())
            for node in nodes:
                with node.lock:
                    if node._pending:
                        return False
            return True

        wait_until(queues_drained, description="drained queues")
        record("soak-drained", metrics=self.metrics("drained"))
        latencies = sorted(self.latencies)
        record(
            "latencies",
            samples=len(latencies),
            p50=latencies[int(len(latencies) * 0.50)],
            p95=latencies[int(len(latencies) * 0.95)],
            p99=latencies[int(len(latencies) * 0.99)],
        )
        record("scenario-pass", scenario="shared-client-soak", duration=duration)

    def reconnects(self):
        record("scenario-start", scenario="lifecycle-reconnect")
        samples = []
        for iteration in range(5):
            start = time.monotonic()
            self.client.disconnect()
            assert not self.allocator.worker.is_alive()
            assert not self.allocator.worker.poll.is_alive()
            record(
                "client-closed", iteration=iteration, elapsed=time.monotonic() - start
            )
            self.client.connect(self.zk_proxy.hosts, self.code)
            self.locator.zk.add_listener(self.zk_state)
            self.round_trip(PREFIX + f":lifecycle:{iteration}")
            samples.append(self.metrics("reconnected"))
        assert samples[-1]["fds"] <= samples[0]["fds"] + 2, samples
        assert samples[-1]["threads"] <= samples[0]["threads"] + 2, samples
        record("scenario-pass", scenario="lifecycle-reconnect", samples=samples)

    def close(self):
        cleanup = []
        if hasattr(self, "client"):
            cleanup.append(self.client.disconnect)
        elif hasattr(self, "allocator"):
            cleanup.append(self.allocator.close)
        if hasattr(self, "admin"):
            cleanup.extend([self.admin.stop, self.admin.close])
        cleanup.extend(proxy.close for proxy in getattr(self, "proxies", {}).values())
        if hasattr(self, "zk_proxy"):
            cleanup.append(self.zk_proxy.close)
        first_error = None
        for close in cleanup:
            try:
                close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error
        if hasattr(self, "allocator"):
            assert not self.allocator.worker.is_alive()
            assert not self.allocator.worker.poll.is_alive()


def main():
    assert hasattr(select, "epoll"), "Linux epoll is required"
    logging.basicConfig(level=logging.WARNING)
    record(
        "configuration",
        python=os.sys.version,
        prefix=PREFIX,
        ttl=TTL,
        connect_timeout=CONNECT_TIMEOUT,
        io_timeout=IO_TIMEOUT,
        operation_timeout=OPERATION_TIMEOUT,
        result_wait=WAIT_TIMEOUT,
        soak_seconds=int(os.environ.get("ARCUS_STABILITY_SOAK_SECONDS", "120")),
    )
    harness = None
    status = "failed"
    try:
        harness = Harness.__new__(Harness)
        harness.__init__()
        harness.concurrent_clients()
        harness.network_faults()
        harness.membership_faults()
        harness.zookeeper_faults()
        harness.concurrent_soak(
            int(os.environ.get("ARCUS_STABILITY_SOAK_SECONDS", "120"))
        )
        harness.reconnects()
        status = "passed"
    except Exception:
        record("failure", traceback=traceback.format_exc())
        raise
    finally:
        try:
            if harness is not None:
                harness.close()
        except Exception:
            status = "failed"
            raise
        finally:
            record("run-finished", status=status)
            EVENTS.close()
            REQUESTS.close()
            METRICS.close()


if __name__ == "__main__":
    main()
