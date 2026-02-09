#!/usr/bin/env python3
"""
Figure 3: Post Popularity Metrics

Two-panel figure showing:
a) Average upvotes vs discussion tree size
b) Average direct replies vs discussion tree size

Replicates Medvedev et al. (2018) Figure 3 analysis.
"""
import sqlite3
import argparse
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

from figure_style import (
    setup_style, COLORS, add_panel_label, format_axis_log,
    fit_power_law, get_spam_post_ids
)

DEFAULT_DB = "data/moltbook_paper.db"


def get_post_metrics(conn, spam_ids):
    """Get upvotes, tree size, and direct replies for each post.

    Returns two lists:
    - all_metrics: (upvotes, tree_size, direct_replies) for all posts
    - complete_metrics: same but only for posts with complete comments (stored == API count)
    """
    # Get post upvotes, comment counts, and actual stored counts
    posts = conn.execute('''
        SELECT p.id, p.upvotes, p.comment_count, COALESCE(c.stored_count, 0) as stored
        FROM posts p
        LEFT JOIN (
            SELECT post_id, COUNT(*) as stored_count
            FROM comments
            GROUP BY post_id
        ) c ON p.id = c.post_id
        WHERE p.upvotes IS NOT NULL
          AND p.comment_count IS NOT NULL
          AND p.comment_count > 0
    ''').fetchall()

    # Get direct reply counts (comments with no parent)
    direct_replies = conn.execute('''
        SELECT post_id, COUNT(*) as direct_count
        FROM comments
        WHERE parent_id IS NULL
        GROUP BY post_id
    ''').fetchall()
    direct_map = {r[0]: r[1] for r in direct_replies}

    # Filter out spam posts and separate complete vs all
    all_metrics = []
    complete_metrics = []
    for post_id, upvotes, api_count, stored_count in posts:
        if post_id in spam_ids:
            continue
        direct = direct_map.get(post_id, 0)
        all_metrics.append((upvotes, api_count, direct))
        # Complete = stored count matches API count
        if stored_count == api_count:
            complete_metrics.append((upvotes, stored_count, direct))

    return all_metrics, complete_metrics


def bin_and_average(x_vals, y_vals, num_bins=30):
    """Bin x values logarithmically and compute average y per bin with 10-90% quantiles."""
    x_vals = np.array(x_vals)
    y_vals = np.array(y_vals)

    # Filter positive values
    mask = (x_vals > 0) & (y_vals >= 0)
    x_vals = x_vals[mask]
    y_vals = y_vals[mask]

    if len(x_vals) < 10:
        return np.array([]), np.array([]), np.array([]), np.array([])

    # Create logarithmic bins
    log_bins = np.logspace(np.log10(x_vals.min()), np.log10(x_vals.max()), num_bins)

    bin_centers = []
    bin_means = []
    bin_lo = []
    bin_hi = []

    for i in range(len(log_bins) - 1):
        mask = (x_vals >= log_bins[i]) & (x_vals < log_bins[i + 1])
        if mask.sum() >= 5:  # Require at least 5 points per bin
            bin_centers.append(np.sqrt(log_bins[i] * log_bins[i + 1]))
            bin_means.append(np.mean(y_vals[mask]))
            bin_lo.append(np.percentile(y_vals[mask], 10))
            bin_hi.append(np.percentile(y_vals[mask], 90))

    return np.array(bin_centers), np.array(bin_means), np.array(bin_lo), np.array(bin_hi)


def main():
    parser = argparse.ArgumentParser(description='Figure 3: Post Popularity')
    parser.add_argument('--db', type=str, default=DEFAULT_DB)
    parser.add_argument('--output', type=str, default='figures/figure3_popularity.pdf')
    args = parser.parse_args()

    setup_style()
    conn = sqlite3.connect(args.db)

    # Get spam post IDs
    spam_ids = get_spam_post_ids(conn)
    print(f'Spam posts to exclude: {len(spam_ids):,}')

    # Load data
    all_metrics, complete_metrics = get_post_metrics(conn, spam_ids)
    print(f'All posts with metrics: {len(all_metrics):,}')
    print(f'Posts with complete comments: {len(complete_metrics):,}')

    # Left panel uses all posts
    upvotes = np.array([m[0] for m in all_metrics])
    tree_sizes = np.array([m[1] for m in all_metrics])

    # Right panel uses only posts with complete comments
    tree_sizes_complete = np.array([m[1] for m in complete_metrics])
    direct_replies_complete = np.array([m[2] for m in complete_metrics])

    # Create figure
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    # Panel a: Upvotes vs tree size
    ax = axes[0]

    # Bin and average
    x_bins, y_means, y_lo, y_hi = bin_and_average(tree_sizes, upvotes)

    if len(x_bins) > 0:
        yerr_lo = y_means - y_lo
        yerr_hi = y_hi - y_means
        ax.errorbar(x_bins, y_means, yerr=[yerr_lo, yerr_hi], fmt='o-', color=COLORS['blue'],
                    markersize=6, linewidth=1.5, capsize=2, alpha=0.8,
                    label='AI agents (10-90%)')

        # Fit power law
        mask = x_bins >= 2
        if mask.sum() >= 3:
            exp, pref = fit_power_law(x_bins[mask], y_means[mask])
            if exp is not None:
                x_fit = np.logspace(np.log10(x_bins[mask].min()),
                                   np.log10(x_bins[mask].max()), 50)
                ax.loglog(x_fit, pref * x_fit**exp, '--', color=COLORS['grey'],
                          linewidth=2, label=f'β = {exp:.2f}')

    ax.set_xlabel('Discussion tree size (comments)')
    ax.set_ylabel('Average upvotes')
    ax.legend(loc='upper left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'a')

    # Panel b: Direct replies vs tree size (only posts with complete comments)
    ax = axes[1]

    # Bin and average (using only complete posts)
    x_bins, y_means, y_lo, y_hi = bin_and_average(tree_sizes_complete, direct_replies_complete)

    if len(x_bins) > 0:
        yerr_lo = y_means - y_lo
        yerr_hi = y_hi - y_means
        ax.errorbar(x_bins, y_means, yerr=[yerr_lo, yerr_hi], fmt='o-', color=COLORS['green'],
                    markersize=6, linewidth=1.5, capsize=2, alpha=0.8,
                    label='AI agents (10-90%)')

        # Fit power law only up to tree size 100
        mask = (x_bins >= 2) & (x_bins <= 100)
        if mask.sum() >= 3:
            exp, pref = fit_power_law(x_bins[mask], y_means[mask])
            if exp is not None:
                # Extend fit line beyond data range for visibility
                x_fit = np.logspace(np.log10(x_bins[mask].min()),
                                   np.log10(x_bins.max()) + 0.3, 50)
                ax.loglog(x_fit, pref * x_fit**exp, '--', color=COLORS['grey'],
                          linewidth=2, label=f'β = {exp:.2f} (fit ≤100)')

    ax.set_xlabel('Discussion tree size (comments)')
    ax.set_ylabel('Average direct replies')
    ax.legend(loc='upper left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'b')

    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f'\nSaved: {args.output}')

    # Print summary stats
    print(f'\n=== Summary ===')
    print(f'Median upvotes: {np.median(upvotes):.0f}')
    print(f'Median tree size (all): {np.median(tree_sizes):.0f}')
    print(f'Median tree size (complete): {np.median(tree_sizes_complete):.0f}')
    print(f'Median direct replies (complete): {np.median(direct_replies_complete):.0f}')

    conn.close()


if __name__ == "__main__":
    main()
