"""Summarize recorded experiments without converting smoke results into SLAs."""

import argparse
from collections import Counter
import json
from pathlib import Path


def read_lines(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def bounds(samples, name):
    values = [sample[name] for sample in samples]
    return (
        {"first": values[0], "last": values[-1], "min": min(values), "max": max(values)}
        if values
        else None
    )


def summarize(directory):
    events = read_lines(directory / "events.jsonl")
    requests = read_lines(directory / "requests.jsonl")
    metrics = read_lines(directory / "metrics.jsonl")
    submitted = {
        request["id"] for request in requests if request["status"] == "submitted"
    }
    terminal = [request for request in requests if request["status"] != "submitted"]
    completions = Counter(request["id"] for request in terminal)
    errors = Counter(
        request.get("error") for request in terminal if request["status"] == "error"
    )
    starts = [
        event
        for event in events
        if event["kind"] == "scenario-start"
        and event.get("scenario") == "shared-client-soak"
    ]
    ends = [
        event
        for event in events
        if event["kind"] == "scenario-pass"
        and event.get("scenario") == "shared-client-soak"
    ]
    soak = None
    if starts and ends:
        start, end = starts[-1]["monotonic"], ends[-1]["monotonic"]
        attempts = [
            request
            for request in terminal
            if start <= request["submitted"] <= request["completed"] <= end
        ]
        latencies = sorted(request["elapsed"] * 1000 for request in attempts)
        samples = [sample for sample in metrics if sample["label"] == "soak"]
        warmed = [
            sample for sample in samples if sample["time"] >= starts[-1]["time"] + 30
        ]
        soak = {
            "seconds": end - start,
            "attempts": len(attempts),
            "errors": sum(request["status"] == "error" for request in attempts),
            "completed_attempts_per_second": len(attempts) / (end - start),
            "latency_ms": {
                f"p{percentile}": latencies[
                    min(len(latencies) - 1, int(len(latencies) * percentile / 100))
                ]
                for percentile in [50, 95, 99]
            }
            if latencies
            else None,
            "metrics": {
                name: bounds(samples, name)
                for name in [
                    "rss_kib",
                    "fds",
                    "threads",
                    "worker_queue",
                    "pending_operations",
                    "cpu_seconds",
                ]
            },
            "rss_after_30s": bounds(warmed, "rss_kib"),
        }
    finished = [event for event in events if event["kind"] == "run-finished"]
    lifecycle = [
        event
        for event in events
        if event["kind"] == "scenario-pass"
        and event.get("scenario") == "lifecycle-reconnect"
    ]
    summary = {
        "status": finished[-1]["status"] if finished else "interrupted",
        "passed_scenarios": [
            event["scenario"] for event in events if event["kind"] == "scenario-pass"
        ],
        "terminal_attempts": len(terminal),
        "errors_by_type": dict(errors),
        "submission_records": len(submitted),
        "unfinished_request_ids": sorted(submitted - completions.keys()),
        "duplicate_terminal_ids": sorted(
            request_id for request_id, count in completions.items() if count > 1
        ),
        "soak_smoke": soak,
        "drained": [
            event["metrics"] for event in events if event["kind"] == "soak-drained"
        ],
        "zk_expiry_evidence": [
            event for event in events if event["kind"] == "zk-expiry-evidence"
        ],
        "membership_traffic": [
            event for event in events if event["kind"] == "membership-traffic-drained"
        ],
        "lifecycle": {
            name: bounds(lifecycle[-1]["samples"], name)
            for name in ["fds", "threads", "rss_kib"]
        }
        if lifecycle
        else None,
        "production_soak_qualified": False,
        "limitation": "Bounded smoke with test proxies; production duration and SLOs remain undefined.",
    }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.directory), indent=2))
