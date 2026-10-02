"""Host-side controller restricted to a dedicated Compose project."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "arcus-python-stability"


def main():
    def terminate(signum, frame):
        raise KeyboardInterrupt("Termination requested")

    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("--soak-seconds", type=int, default=120)
    args = parser.parse_args()
    if not 1 <= args.soak_seconds <= 1200:
        parser.error("--soak-seconds must be between 1 and 1200")
    results = (
        ROOT
        / "build"
        / "stability"
        / f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}"
    )
    results.mkdir(parents=True)
    env = dict(
        os.environ,
        ARCUS_TEST_RESULTS=str(results),
        ARCUS_STABILITY_SOAK_SECONDS=str(args.soak_seconds),
    )
    compose = [
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "-f",
        str(ROOT / "compose.yaml"),
        "-f",
        str(ROOT / "stability/compose.yaml"),
    ]

    def run(*command, **kwargs):
        return subprocess.run(
            compose + list(command), env=env, check=True, timeout=180, **kwargs
        )

    if run("ps", "--all", "--quiet", capture_output=True, text=True).stdout.strip():
        raise SystemExit(f"Refusing to replace existing {PROJECT} containers")
    test = None
    status = 1
    try:
        with (results / "environment.json").open("w") as stream:
            json.dump(
                {
                    "project": PROJECT,
                    "revision": subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], text=True
                    ).strip(),
                    "dirty": bool(
                        subprocess.check_output(
                            ["git", "status", "--porcelain"], text=True
                        ).strip()
                    ),
                    "soak_seconds": args.soak_seconds,
                    "source_sha256": {
                        str(path.relative_to(ROOT)): hashlib.sha256(
                            path.read_bytes()
                        ).hexdigest()
                        for path in sorted((ROOT / "src").rglob("*.py"))
                    },
                },
                stream,
                indent=2,
            )
        run("build", "tests")
        run("build", "stability")
        run("up", "--detach", "register")
        run("up", "--detach", "--wait", "--wait-timeout", "120", "cache1", "cache2")
        with (results / "client.log").open("w") as log:
            test = subprocess.Popen(
                compose + ["run", "--rm", "--no-deps", "-T", "stability"],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + args.soak_seconds + 420
            handled = None
            stats_at = 0.0
            while test.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "External watchdog expired; this is a failed run"
                    )
                if time.monotonic() >= stats_at:
                    stats_at = time.monotonic() + 5
                    ids = run(
                        "ps",
                        "--quiet",
                        "cache1",
                        "cache2",
                        "zookeeper",
                        capture_output=True,
                        text=True,
                    ).stdout.split()
                    if ids:
                        stats = subprocess.run(
                            [
                                "docker",
                                "stats",
                                "--no-stream",
                                "--format",
                                "{{json .}}",
                                *ids,
                            ],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )
                        with (results / "server-metrics.jsonl").open("a") as stream:
                            stream.write(
                                json.dumps(
                                    {
                                        "time": time.time(),
                                        "stats": stats.stdout.splitlines(),
                                        "status": stats.returncode,
                                    }
                                )
                                + "\n"
                            )
                request_path = results / "fault-request.json"
                try:
                    request = json.loads(request_path.read_text())
                except (FileNotFoundError, json.JSONDecodeError):
                    request = None
                if request is not None:
                    if request["id"] != handled:
                        handled = request["id"]
                        action, service = request["action"], request["service"]
                        if action not in {"stop", "kill", "start"} or service not in {
                            "cache1",
                            "cache2",
                        }:
                            raise ValueError("Refusing an unexpected fault command")
                        print(f"Fault {handled}: {action} {service}", flush=True)
                        command = (
                            [action, service]
                            if action != "stop"
                            else ["stop", "--timeout", "5", service]
                        )
                        run(*command)
                        reply = results / "fault-done.tmp"
                        reply.write_text(
                            json.dumps(
                                {
                                    "id": handled,
                                    "action": action,
                                    "service": service,
                                    "completed": time.time(),
                                }
                            )
                        )
                        reply.replace(results / "fault-done.json")
                time.sleep(0.1)
            status = test.returncode
    finally:
        try:
            if test is not None and test.poll() is None:
                test.terminate()
                try:
                    test.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    test.kill()
                    test.wait(timeout=5)
            try:
                with (results / "services.log").open("w") as log:
                    subprocess.run(
                        compose + ["logs", "--no-color", "--timestamps"],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=30,
                    )
            except (OSError, subprocess.SubprocessError) as error:
                status = 1
                print(f"Service log collection failed: {error}", file=sys.stderr)
        finally:
            # Cleanup must run even if the client process or diagnostics fail.
            subprocess.run(
                compose + ["down", "--volumes", "--remove-orphans", "--timeout", "10"],
                env=env,
                check=True,
                timeout=60,
            )
        if (results / "client.log").exists():
            print((results / "client.log").read_text())
        subprocess.run(
            [sys.executable, str(ROOT / "stability/summarize.py"), str(results)],
            check=True,
            timeout=30,
        )
        print(f"Stability results: {results}", flush=True)
    raise SystemExit(status)


if __name__ == "__main__":
    main()
