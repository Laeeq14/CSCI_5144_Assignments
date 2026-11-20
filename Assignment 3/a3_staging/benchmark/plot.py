# Generate throughput, latency, and scaling charts from bench.py results.csv.
# Run: py benchmark/plot.py

import argparse
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def load(path):
    with open(path) as f:
        return [{k: (float(v) if k != "phase" else v) for k, v in r.items()}
                for r in csv.DictReader(f)]


def by_phase(rows, phase):
    return sorted([r for r in rows if r["phase"] == phase], key=lambda r: r["nodes"])


def bar_chart(ax, x, y, label, color, width=0.3, offset=0):
    bars = ax.bar(np.array(x) + offset, y, width, label=label, color=color,
                  edgecolor="black", linewidth=0.6)
    for bar, val in zip(bars, y):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.0f}", ha="center", va="bottom", fontsize=8)


def xtick_labels(nodes):
    return [f"{n} node{'s' if n > 1 else ''}" for n in nodes]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=os.path.join(os.path.dirname(__file__), "results.csv"))
    args   = parser.parse_args()

    rows   = load(args.csv)
    sets   = by_phase(rows, "SET")
    gets   = by_phase(rows, "GET")
    nodes  = [int(r["nodes"]) for r in sets]
    outdir = os.path.dirname(args.csv)

    # Throughput
    fig, ax = plt.subplots(figsize=(7, 4))
    bar_chart(ax, nodes, [r["tput"] for r in sets], "SET", "#4C72B0", offset=-0.17)
    bar_chart(ax, nodes, [r["tput"] for r in gets], "GET", "#DD8452", offset=+0.17)
    ax.set_xlabel("Number of KV Store Nodes")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title("Throughput vs. Number of Nodes")
    ax.set_xticks(nodes)
    ax.set_xticklabels(xtick_labels(nodes))
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    out1 = os.path.join(outdir, "throughput.png")
    fig.savefig(out1, dpi=150)
    print(f"Saved {out1}")
    plt.close(fig)

    # Latency percentiles
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, phase_rows, label in [(axes[0], sets, "SET"), (axes[1], gets, "GET")]:
        x = np.array(nodes)
        w = 0.18
        bar_chart(ax, x, [r["avg_ms"] for r in phase_rows], "avg", "#4C72B0", w, offset=-1.5*w)
        bar_chart(ax, x, [r["p50_ms"] for r in phase_rows], "p50", "#55A868", w, offset=-0.5*w)
        bar_chart(ax, x, [r["p95_ms"] for r in phase_rows], "p95", "#DD8452", w, offset=+0.5*w)
        bar_chart(ax, x, [r["p99_ms"] for r in phase_rows], "p99", "#C44E52", w, offset=+1.5*w)
        ax.set_xlabel("Number of KV Store Nodes")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{label} Latency Percentiles")
        ax.set_xticks(nodes)
        ax.set_xticklabels(xtick_labels(nodes))
        ax.legend(fontsize=8)
        ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    out2 = os.path.join(outdir, "latency.png")
    fig.savefig(out2, dpi=150)
    print(f"Saved {out2}")
    plt.close(fig)

    # Scaling factor
    fig, ax = plt.subplots(figsize=(6, 4))
    base_set  = sets[0]["tput"]
    base_get  = gets[0]["tput"]
    scale_set = [r["tput"] / base_set for r in sets]
    scale_get = [r["tput"] / base_get for r in gets]
    ax.plot(nodes, scale_set, "o-", label="SET scaling", color="#4C72B0")
    ax.plot(nodes, scale_get, "s-", label="GET scaling", color="#DD8452")
    ax.plot(nodes, nodes,     "--", label="Ideal linear", color="gray", alpha=0.6)
    for n, s, g in zip(nodes, scale_set, scale_get):
        ax.annotate(f"{s:.2f}x", (n, s), textcoords="offset points", xytext=(5,  4), fontsize=8, color="#4C72B0")
        ax.annotate(f"{g:.2f}x", (n, g), textcoords="offset points", xytext=(5, -12), fontsize=8, color="#DD8452")
    ax.set_xlabel("Number of KV Store Nodes")
    ax.set_ylabel("Scaling Factor (relative to 1 node)")
    ax.set_title("Throughput Scaling Efficiency")
    ax.set_xticks(nodes)
    ax.legend()
    ax.grid(linestyle="--", alpha=0.5)
    fig.tight_layout()
    out3 = os.path.join(outdir, "scaling.png")
    fig.savefig(out3, dpi=150)
    print(f"Saved {out3}")
    plt.close(fig)


if __name__ == "__main__":
    main()
