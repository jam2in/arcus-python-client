"""Closed-loop completion-latency benchmark; results are measurements, not SLOs."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import queue
import resource
import subprocess
import threading
import time
from uuid import uuid4

import psutil
from pymemcache.client.base import Client

from arcus import Arcus, ArcusLocator, ArcusTranscoder
from arcus_mc_node import ArcusMCNodeAllocator

TTL = 300
TIMEOUT = 5
RESULTS = Path("/results")


def done(operation):
    return operation.get_result(timeout=TIMEOUT)


class Adapter:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        if name == "arcus-python":
            self.client = Arcus(ArcusLocator(ArcusMCNodeAllocator(ArcusTranscoder())))
            self.client.connect("zookeeper:2181", "python-benchmark")
            assert len(self.client.locator.addr_node_map) == 1
        else:
            # One reused socket for both clients. The lock's wait is measured.
            self.client = Client(
                ("cache1", 11211),
                connect_timeout=1,
                timeout=TIMEOUT,
                no_delay=False,
                default_noreply=False,
            )

    def set(self, key, value):
        if self.name == "arcus-python":
            return done(self.client.set(key, value, exptime=TTL))
        with self.lock:
            return self.client.set(key, value, expire=TTL, flags=2048, noreply=False)

    def get(self, key):
        if self.name == "arcus-python":
            return done(self.client.get(key))
        with self.lock:
            return self.client.get(key)

    def delete(self, key):
        if self.name == "arcus-python":
            return done(self.client.delete(key))
        with self.lock:
            return self.client.delete(key, noreply=False)

    def close(self):
        if self.name == "arcus-python":
            self.client.disconnect()
        else:
            self.client.close()


def stats():
    with closing(
        Client(("cache1", 11211), connect_timeout=1, timeout=TIMEOUT)
    ) as client:
        return {
            key.decode(): value.decode() if isinstance(value, bytes) else str(value)
            for key, value in client.stats().items()
        }


def percentiles(samples):
    ordered = sorted(duration / 1e6 for duration, status in samples if status == 0)
    if not ordered:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None}
    return {
        f"p{percentile}_ms": ordered[
            max(0, math.ceil(len(ordered) * percentile / 100) - 1)
        ]
        for percentile in (50, 95, 99)
    }


def measure(operation, concurrency, duration):
    start = threading.Barrier(concurrency + 1)
    deadline = [0.0]

    def run(worker):
        samples = []
        index = worker
        start.wait()
        while time.monotonic() < deadline[0]:
            before = time.perf_counter_ns()
            status = 0
            try:
                assert operation(index), "response mismatch"
            except (TimeoutError, queue.Empty):
                status = 3
            except AssertionError:
                status = 2
            except Exception:
                status = 4
            samples.append((time.perf_counter_ns() - before, status))
            index += concurrency
        return samples

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(run, worker) for worker in range(concurrency)]
        before = time.monotonic()
        cpu_before = time.process_time()
        deadline[0] = before + duration
        start.wait()
        samples = [
            sample
            for future in futures
            for sample in future.result(timeout=duration + 15)
        ]
        elapsed = time.monotonic() - before
        cpu = time.process_time() - cpu_before
    return {"samples": samples, "elapsed_s": elapsed, "cpu_s": cpu}


def setup(adapter, operation, keys, payload, elements):
    if operation in {"get", "set", "get-miss", "mixed"}:
        for key in keys:
            assert adapter.set(key, payload) is True
            assert adapter.get(key) == payload

        def invoke(index):
            key = keys[index % len(keys)]
            if operation == "set" or (operation == "mixed" and index % 5 == 0):
                return adapter.set(key, payload) is True
            if operation == "get-miss":
                return adapter.get(key + ":absent") is None
            return adapter.get(key) == payload

        return invoke

    client = adapter.client
    key = keys[0]
    values = [f"{index:08d}".encode() + payload for index in range(elements)]
    if operation == "list-get":
        assert done(client.lop_create(key, 2048, exptime=TTL)) is True
        for value in values:
            assert done(client.lop_insert(key, -1, value)) is True
        return lambda index: done(client.lop_get(key, (0, elements - 1))) == values
    if operation == "set-get":
        assert done(client.sop_create(key, 2048, exptime=TTL)) is True
        for value in values:
            assert done(client.sop_insert(key, value)) is True
        expected = set(values)
        return lambda index: done(client.sop_get(key)) == expected
    if operation == "btree-get":
        assert done(client.bop_create(key, 2048, exptime=TTL)) is True
        for index, value in enumerate(values):
            assert done(client.bop_insert(key, index, value, "0x01")) is True
        expected = {index: ("0x01", value) for index, value in enumerate(values)}
        return lambda index: done(client.bop_get(key, (0, elements - 1))) == expected
    raise ValueError(operation)


def run_python(case, args, prefix):
    adapter = Adapter(case["client"])
    keys = [f"{prefix}:{index}" for index in range(256)]
    if case["operation"].endswith("-get"):
        keys = keys[:1]
    try:
        operation = setup(
            adapter, case["operation"], keys, b"x" * case["size"], case["elements"]
        )
        warmup = measure(operation, case["concurrency"], args.warmup)
        assert all(status == 0 for _, status in warmup["samples"]), "warmup failed"
        before = stats()
        result = measure(operation, case["concurrency"], args.duration)
        result["server_before"] = before
        result["server_after"] = stats()
        process = psutil.Process()
        result["rss_bytes_after"] = process.memory_info().rss
        result["threads_after"] = process.num_threads()
        result["fds_after"] = process.num_fds()
        result["resource_scope"] = (
            "measurement only; RSS/threads/fds sampled after measurement"
        )
        return result
    finally:
        for key in keys:
            adapter.delete(key)
        adapter.close()


def run_java(case, args, prefix):
    raw_path = RESULTS / f"{prefix}-java.json"
    log_path = RESULTS / f"{prefix}-java.log"
    command = [
        "java",
        "-Xms64m",
        "-Xmx256m",
        "-cp",
        "/java/target/classes:/java/target/dependency/*",
        "Benchmark",
        case["operation"],
        prefix,
        str(raw_path),
        str(case["size"]),
        str(case["concurrency"]),
        str(args.duration),
        str(args.warmup),
    ]
    before = stats()
    peak_rss = 0
    with log_path.open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        monitor = psutil.Process(process.pid)
        deadline = time.monotonic() + args.duration + args.warmup + 45
        while process.poll() is None:
            if time.monotonic() >= deadline:
                process.kill()
                process.wait()
                raise TimeoutError("Java runner exceeded deadline")
            try:
                peak_rss = max(peak_rss, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                pass
            time.sleep(0.1)
        if process.returncode:
            raise RuntimeError(f"Java runner failed: {log_path}")
    result = json.loads(raw_path.read_text())
    result.update(
        server_before=before,
        server_after=stats(),
        rss_bytes_peak=peak_rss,
        resource_scope="CPU measurement only; RSS peak/server stats include JVM startup, seed, warmup and cleanup",
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", default="arcus-python,pymemcache,arcus-java")
    parser.add_argument("--operations", default="get,set")
    parser.add_argument("--sizes", default="64,1024,4096")
    parser.add_argument("--concurrency", default="1,4")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--collections", action="store_true")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    assert args.duration > 0 and args.warmup > 0 and args.repeats > 0
    allowed_clients = {"arcus-python", "pymemcache", "arcus-java"}
    allowed_operations = {"get", "set", "get-miss", "mixed"}
    if not set(args.clients.split(",")) <= allowed_clients:
        parser.error("unsupported client")
    if not set(args.operations.split(",")) <= allowed_operations:
        parser.error("unsupported operation")
    if "arcus-java" in args.clients.split(",") and not set(
        args.operations.split(",")
    ) <= {"get", "set"}:
        parser.error("Java supports get and set only")
    if (
        min(map(int, args.sizes.split(","))) < 1
        or min(map(int, args.concurrency.split(","))) < 1
    ):
        parser.error("sizes and concurrency must be positive")
    RESULTS.mkdir(parents=True, exist_ok=True)
    if args.profile:
        command = [
            "py-spy",
            "record",
            "--rate",
            "99",
            "--format",
            "speedscope",
            "--output",
            "/results/profile.json",
            "--",
            "python",
            "runner.py",
            "--clients",
            "arcus-python",
            "--operations",
            "get",
            "--sizes",
            "1024",
            "--concurrency",
            "4",
            "--repeats",
            "1",
            "--duration",
            "15",
            "--warmup",
            "2",
        ]
        subprocess.run(command, check=True, timeout=60)
        return
    run_id = uuid4().hex[:12]
    metadata = {
        "run_id": run_id,
        "configuration": vars(args),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "packages": {
            name: version(name)
            for name in ("arcus-python-client", "pymemcache", "psutil", "py-spy")
        },
        "java_client": "com.navercorp.arcus:arcus-java-client:1.13.2",
        "java_zookeeper_client": "3.9.4 (overrides the published release's old dependency)",
        "cache_connections": 1,
        "control_connection": "temporary stats connection outside measurement",
        "keys": 256,
        "distribution": "deterministic uniform cycling",
        "ttl_seconds": TTL,
        "reply": True,
        "wire_flags": 2048,
        "compression": False,
        "tcp_nodelay": False,
        "load_model": "closed loop, each worker has at most one incomplete request; no coordinated-omission correction",
        "slo": "unspecified; these measurements do not establish production capacity",
    }
    (RESULTS / f"{run_id}-environment.json").write_text(json.dumps(metadata, indent=2))
    cases = []
    for repeat in range(args.repeats):
        clients = args.clients.split(",")
        clients = clients[repeat % len(clients) :] + clients[: repeat % len(clients)]
        for size in map(int, args.sizes.split(",")):
            for concurrency in map(int, args.concurrency.split(",")):
                for operation in args.operations.split(","):
                    for client in clients:
                        cases.append(
                            dict(
                                client=client,
                                operation=operation,
                                size=size,
                                concurrency=concurrency,
                                repeat=repeat,
                                elements=0,
                            )
                        )
        if args.collections:
            for elements in (16, 128):
                for operation in ("list-get", "set-get", "btree-get"):
                    cases.append(
                        dict(
                            client="arcus-python",
                            operation=operation,
                            size=64,
                            concurrency=1,
                            repeat=repeat,
                            elements=elements,
                        )
                    )
    failed = False
    for index, case in enumerate(cases):
        prefix = f"bench:{run_id}:{index}"
        runner = run_java if case["client"] == "arcus-java" else run_python
        result = runner(case, args, prefix)
        samples = result.pop("samples")
        counts = Counter(status for _, status in samples)
        success = counts[0]
        result.update(
            case,
            run_id=run_id,
            case_index=index,
            issued=len(samples),
            succeeded=success,
            errors=len(samples) - success,
            timeouts=counts[3],
            incomplete=0,
            errors_by_code=dict(counts),
            successful_ops_s=success / result["elapsed_s"],
            **percentiles(samples),
        )
        raw_name = f"{run_id}-{index:03d}-samples.json"
        (RESULTS / raw_name).write_text(json.dumps(samples, separators=(",", ":")))
        result["raw_samples"] = raw_name
        with (RESULTS / f"{run_id}-summary.jsonl").open("a") as output:
            output.write(json.dumps(result) + "\n")
        failed |= bool(result["errors"])
        print(
            json.dumps(
                {
                    key: result[key]
                    for key in (
                        "case_index",
                        "client",
                        "operation",
                        "size",
                        "concurrency",
                        "repeat",
                        "successful_ops_s",
                        "p99_ms",
                        "errors",
                    )
                }
            ),
            flush=True,
        )
    if failed:
        raise SystemExit("Benchmark recorded failed requests; inspect raw results")


if __name__ == "__main__":
    main()
