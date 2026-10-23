"""
Parse multi-run loadgen sweep output and plot throughput vs. concurrent goroutines.

Run the sweep as (PowerShell, from kv_store/):
    & {
        foreach ($g in @(1,2,4,8,16,32,50,100,200)) {
            foreach ($run in @(1,2,3)) {
                Write-Output "=== goroutines=$g run=$run ==="
                go run ./loadgen -goroutines $g -ops 5000 -phase both
            }
        }
    } | Tee-Object sweep_results.txt

Usage:
    py plot_throughput.py sweep_results.txt
    # or pipe: <command> | py plot_throughput.py -

Dependencies: pip install matplotlib
"""
import re
import sys
import statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from collections import defaultdict


# parse: returns dict goroutines -> [throughput, ...]
def parse(text):
    results = defaultdict(list)
    current_g = None
    for line in text.splitlines():
        m = re.search(r'goroutines=(\d+)', line)
        if m:
            current_g = int(m.group(1))
        # Unicode arrow U+2192 or ASCII ->
        m = re.search(r'SERVER CAPACITY:.*?[\u2192>]\s*([\d,]+(?:\.\d+)?)\s*ops/s', line)
        if m and current_g is not None:
            results[current_g].append(float(m.group(1).replace(',', '')))
    return results


src = sys.stdin.read() if len(sys.argv) < 2 or sys.argv[1] == '-' \
      else open(sys.argv[1], encoding='utf-8').read()

data = parse(src)
if not data:
    sys.exit("No data found. Make sure the input contains loadgen output.")

goroutines = sorted(data.keys())
medians = [statistics.median(data[g]) for g in goroutines]
mins    = [min(data[g])               for g in goroutines]
maxs    = [max(data[g])               for g in goroutines]
runs    = [len(data[g])               for g in goroutines]

err_lo = [m - mn for m, mn in zip(medians, mins)]
err_hi = [mx - m for m, mx in zip(medians, maxs)]

# plot
fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=150)
fig.patch.set_facecolor('#0f1117')
ax.set_facecolor('#1a1d27')

med_k = [t / 1000 for t in medians]
lo_k  = [e / 1000 for e in err_lo]
hi_k  = [e / 1000 for e in err_hi]

ax.errorbar(goroutines, med_k,
            yerr=[lo_k, hi_k],
            color='#7c6af7', linewidth=2.2, marker='o',
            markerfacecolor='#c4b5fd', markeredgecolor='#7c6af7',
            markersize=7, zorder=3,
            ecolor='#94a3b8', elinewidth=1.2, capsize=4, capthick=1.2,
            label=f'Median ({runs[0]} runs/point, bars = min\u2013max)')

ax.fill_between(goroutines,
                [m - l for m, l in zip(med_k, lo_k)],
                [m + h for m, h in zip(med_k, hi_k)],
                alpha=0.10, color='#7c6af7', zorder=2)

# peak annotation
peak_g   = goroutines[medians.index(max(medians))]
peak_t_k = max(medians) / 1000
ax.annotate(f'peak {peak_t_k:.0f}k ops/s\n@ {peak_g} goroutines\n(still rising)',
            xy=(peak_g, peak_t_k),
            xytext=(peak_g * 0.5, peak_t_k * 0.80),
            color='#e2e8f0', fontsize=8.5,
            arrowprops=dict(arrowstyle='->', color='#94a3b8', lw=1.2),
            bbox=dict(boxstyle='round,pad=0.3', fc='#2d3148', ec='#4a5080', alpha=0.9))

# Python client measured ceiling (observed median from kv_bench.py)
PYTHON_K = 2.101
ax.axhline(PYTHON_K, color='#f59e0b', linewidth=1.2, linestyle=':', zorder=2)
ax.text(goroutines[0] * 1.05, PYTHON_K + peak_t_k * 0.018,
        f'Python kv_bench.py, observed median {PYTHON_K:.1f}k ops/s',
        color='#f59e0b', fontsize=7.5, va='bottom')

ax.set_xscale('log', base=2)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: int(x)))
ax.set_xlabel('Concurrent goroutines (log\u2082 scale)', color='#94a3b8', fontsize=10)
ax.set_ylabel('Throughput (k ops/s)', color='#94a3b8', fontsize=10)
ax.set_title(
    'KV Store \u2014 Server Throughput vs. Concurrent Goroutines\n'
    'Go load generator \u00b7 SET + GET \u00b7 5000 ops/goroutine \u00b7 '
    'client and server on same host \u00b7 Windows 11 \u00b7 Go 1.27.0',
    color='#e2e8f0', fontsize=10, pad=12)

ax.tick_params(colors='#64748b', labelsize=8.5)
for spine in ax.spines.values():
    spine.set_edgecolor('#2d3148')
ax.grid(True, color='#2d3148', linewidth=0.7, linestyle='--', zorder=1)
ax.legend(fontsize=8, labelcolor='#94a3b8',
          facecolor='#1a1d27', edgecolor='#2d3148')

plt.tight_layout()
out = 'throughput_vs_concurrency.png'
plt.savefig(out, bbox_inches='tight', facecolor=fig.get_facecolor())
print(f"Saved {out}")

# summary table
print(f"\n{'Goroutines':>12}  {'Runs':>5}  {'Median ops/s':>14}  {'Min':>10}  {'Max':>10}")
print('-' * 58)
for g, med, mn, mx, n in zip(goroutines, medians, mins, maxs, runs):
    print(f"{g:>12}  {n:>5}  {med:>14,.0f}  {mn:>10,.0f}  {mx:>10,.0f}")
