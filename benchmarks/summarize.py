"""Summarize repeated benchmark rows without mixing different scenarios."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import median


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    groups = defaultdict(list)
    for file in sorted(args.directory.glob("*-summary.jsonl")):
        for line in file.read_text().splitlines():
            row = json.loads(line)
            if row.get("profile_mode"):
                continue
            samples = json.loads((args.directory / row["raw_samples"]).read_text())
            row["max_ms"] = max((sample[0] / 1e6 for sample in samples), default=0)
            key = tuple(
                row[field]
                for field in ("client", "operation", "size", "concurrency", "elements")
            )
            groups[key].append(row)
    print(
        "| Client | Operation | Bytes | Workers | Elements | Runs | Success ops/s median [min–max] | p99 ms median | Worst observed ms | Errors | Client CPU cores |"
    )
    print(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    for key, rows in sorted(groups.items()):
        rates = [row["successful_ops_s"] for row in rows]
        p99s = [row["p99_ms"] for row in rows if row["p99_ms"] is not None]
        cpu = median(row["cpu_s"] / row["elapsed_s"] for row in rows)
        columns = (
            *key,
            len(rows),
            f"{median(rates):.0f} [{min(rates):.0f}–{max(rates):.0f}]",
            f"{median(p99s):.3f}" if p99s else "none",
            f"{max(row['max_ms'] for row in rows):.3f}",
            sum(row["errors"] for row in rows),
            f"{cpu:.2f}",
        )
        print("| " + " | ".join(map(str, columns)) + " |")
    if not groups:
        raise SystemExit("No completed scenario summaries found")


if __name__ == "__main__":
    main()
