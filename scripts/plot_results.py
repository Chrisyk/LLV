#!/usr/bin/env python3
"""
VLL Benchmark Results Plotter

Usage:
    python3 plot_results.py [output_prefix]

    output_prefix: Base name for CSV files (default: benchmark_results)
                   Expects files: {prefix}_2pl.csv, {prefix}_vll.csv, {prefix}_vll_sca.csv
"""

import sys
import matplotlib.pyplot as plt
import numpy as np
import csv
from pathlib import Path


def load_csv(filename):
    """Load CSV data and return contention_index and throughput arrays."""
    ci, tps = [], []
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ci.append(float(row['contention_index']))
            tps.append(float(row['throughput_tps']))
    return np.array(ci), np.array(tps)


def setup_plot_style():
    """Configure publication-quality plot settings."""
    plt.rcParams.update({
        'font.size': 12,
        'font.family': 'serif',
        'axes.labelsize': 14,
        'axes.titlesize': 14,
        'legend.fontsize': 11,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
        'figure.figsize': (8, 5),
        'figure.dpi': 150,
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
    })


def plot_throughput(prefix, ci_2pl, tps_2pl, ci_vll, tps_vll, ci_vll_sca, tps_vll_sca):
    """Create line plot of throughput vs contention (similar to VLL paper Figure 4)."""
    fig, ax = plt.subplots()

    ax.plot(ci_2pl, tps_2pl, 'b-s', label='2PL', markersize=6, linewidth=1.5)
    ax.plot(ci_vll, tps_vll, 'r-^', label='VLL', markersize=6, linewidth=1.5)
    ax.plot(ci_vll_sca, tps_vll_sca, 'g-o', label='VLL with SCA', markersize=6, linewidth=1.5)

    ax.set_xscale('log')
    ax.set_xlabel('Contention Index')
    ax.set_ylabel('Throughput (txns/sec)')
    ax.set_title('Transactional Throughput vs. Contention')

    ax.grid(True, which='both', linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)
    ax.legend(loc='upper right')
    ax.set_ylim(bottom=0)

    for ext in ['png', 'pdf']:
        outfile = f'{prefix}_throughput.{ext}'
        plt.savefig(outfile)
        print(f'Saved: {outfile}')

    plt.close()


def plot_bar_chart(prefix, ci_2pl, tps_2pl, ci_vll, tps_vll, ci_vll_sca, tps_vll_sca):
    """Create bar chart comparing protocols at key contention levels."""
    fig, ax = plt.subplots(figsize=(10, 5))

    # Select key contention points
    n = len(ci_2pl)
    key_indices = [0, n//4, n//2, 3*n//4, n-1]
    key_indices = sorted(set(i for i in key_indices if 0 <= i < n))

    x_labels = [f'{ci_2pl[i]:.4f}' for i in key_indices]
    x = np.arange(len(x_labels))
    width = 0.25

    ax.bar(x - width, [tps_2pl[i] for i in key_indices], width, label='2PL', color='steelblue')
    ax.bar(x, [tps_vll[i] for i in key_indices], width, label='VLL', color='indianred')
    ax.bar(x + width, [tps_vll_sca[i] for i in key_indices], width, label='VLL+SCA', color='forestgreen')

    ax.set_xlabel('Contention Index')
    ax.set_ylabel('Throughput (txns/sec)')
    ax.set_title('Throughput Comparison at Key Contention Levels')
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.legend()
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()

    for ext in ['png', 'pdf']:
        outfile = f'{prefix}_bar_chart.{ext}'
        plt.savefig(outfile)
        print(f'Saved: {outfile}')

    plt.close()


def main():
    prefix = sys.argv[1] if len(sys.argv) > 1 else 'benchmark_results'

    # Check for CSV files
    files = {
        '2pl': f'{prefix}_2pl.csv',
        'vll': f'{prefix}_vll.csv',
        'vll_sca': f'{prefix}_vll_sca.csv',
    }

    for name, path in files.items():
        if not Path(path).exists():
            print(f'Error: {path} not found')
            print(f'Usage: {sys.argv[0]} [output_prefix]')
            sys.exit(1)

    print(f'Loading data from {prefix}_*.csv files...')
    ci_2pl, tps_2pl = load_csv(files['2pl'])
    ci_vll, tps_vll = load_csv(files['vll'])
    ci_vll_sca, tps_vll_sca = load_csv(files['vll_sca'])

    setup_plot_style()

    print('Generating plots...')
    plot_throughput(prefix, ci_2pl, tps_2pl, ci_vll, tps_vll, ci_vll_sca, tps_vll_sca)
    plot_bar_chart(prefix, ci_2pl, tps_2pl, ci_vll, tps_vll, ci_vll_sca, tps_vll_sca)

    print('Done!')


if __name__ == '__main__':
    main()
