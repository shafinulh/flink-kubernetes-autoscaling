#!/usr/bin/env python3
"""Compare two exported experiments on shared plots."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt


def load_series(path: Path) -> tuple[list[float], list[float]]:
    xs: list[float] = []
    ys: list[float] = []
    if not path.exists():
        return xs, ys
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        first_ts: float | None = None
        for row in reader:
            value = row.get("value", "")
            if value in ("", "NaN", "nan"):
                continue
            ts = float(row["timestamp"])
            if first_ts is None:
                first_ts = ts
            xs.append((ts - first_ts) / 60.0)
            ys.append(float(value))
    return xs, ys


def label_for(path: Path) -> str:
    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
    return metadata["autoscaler"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--output")
    args = parser.parse_args()

    left = Path(args.left).resolve()
    right = Path(args.right).resolve()

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    for path, color in [(left, "tab:blue"), (right, "tab:orange")]:
        label = label_for(path)
        x, y = load_series(path / "throughput_total_source.csv")
        axes[0].plot(x, y, label=label, color=color)

        x, y = load_series(path / "join_busy_pct.csv")
        axes[1].plot(x, y, label=f"{label} busy%", color=color)

        x1, y1 = load_series(path / "kafka_lag_bid.csv")
        x2, y2 = load_series(path / "kafka_lag_auction.csv")
        if y1:
            axes[2].plot(x1, y1, label=f"{label} bid lag", color=color, linestyle="-")
        if y2:
            axes[2].plot(x2, y2, label=f"{label} auction lag", color=color, linestyle=":")

    axes[0].set_title("Total Source Throughput")
    axes[0].set_ylabel("events/s")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    axes[1].set_title("Join Busy Time")
    axes[1].set_ylabel("busy %")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    axes[2].set_title("Kafka Lag")
    axes[2].set_ylabel("events")
    axes[2].set_xlabel("Minutes since experiment start")
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)

    fig.tight_layout()
    output = Path(args.output).resolve() if args.output else Path.cwd() / "compare_justin_ds2.png"
    fig.savefig(output, dpi=180)
    print(output)


if __name__ == "__main__":
    main()
