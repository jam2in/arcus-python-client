"""Describe sampled stacks; sample shares are not CPU cost attribution."""

import argparse
from collections import Counter
import json
from pathlib import Path


def summarize(path):
    data = json.loads(path.read_text())
    frames = data["shared"]["frames"]
    leaf = Counter()
    cumulative = Counter()
    total = 0
    for profile in data["profiles"]:
        if profile["type"] != "sampled":
            continue
        weights = profile.get("weights", [1] * len(profile["samples"]))
        for stack, weight in zip(profile["samples"], weights):
            total += weight
            labels = []
            arcus_labels = set()
            for index in stack:
                frame = frames[index]
                file = frame.get("file", "?")
                label = f"{frame['name']} ({file}:{frame.get('line', '?')})"
                labels.append(label)
                if "/arcus/" in file:
                    arcus_labels.add(label)
            if labels:
                leaf[labels[-1]] += weight
            for label in arcus_labels:
                cumulative[label] += weight
    return {
        "profile": path.name,
        "total_sample_weight": total,
        "leaf_top_percent": [
            (name, value / total * 100) for name, value in leaf.most_common(15)
        ],
        "arcus_cumulative_top_percent": [
            (name, value / total * 100) for name, value in cumulative.most_common(20)
        ],
        "interpretation": "Shares of sampled thread stacks, including native calls and lock waits; not exact CPU time or independent causal evidence",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", nargs="+", type=Path)
    args = parser.parse_args()
    print(json.dumps([summarize(path) for path in args.profiles], indent=2))


if __name__ == "__main__":
    main()
