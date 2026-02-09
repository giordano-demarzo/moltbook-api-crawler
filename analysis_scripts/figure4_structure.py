#!/usr/bin/env python3
"""
Figure 4: Structure of Discussions

Two-panel figure showing:
a) Normalized depth (d/√n) vs normalized width (w/√n) scatter plot
b) Normalized depth distribution histogram (PDF)

Replicates Medvedev et al. (2018) Figure 5 analysis.
Uses actual stored comments. Only includes posts with < 100 API-reported
comments to ensure complete tree structure (API returns max 100 comments).
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


def compute_tree_metrics_batch(conn, spam_ids, min_comments=5, max_comments=100, max_posts=50000):
    """
    Compute tree metrics (depth, width) for posts efficiently.
    Uses actual stored comments. Only includes posts with < max_comments
    API-reported comments to ensure complete tree data.
    Depth 1 = direct reply to post, Depth 2+ = replies to comments.
    """
    # Get posts with actual stored comments >= min_comments AND API count < max_comments
    posts = conn.execute('''
        SELECT c.post_id, COUNT(*) as n
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        WHERE p.comment_count IS NOT NULL AND p.comment_count < ?
        GROUP BY c.post_id
        HAVING n >= ?
        ORDER BY RANDOM()
        LIMIT ?
    ''', (max_comments, min_comments, max_posts * 2)).fetchall()

    # Filter out spam
    post_data = [(p[0], p[1]) for p in posts if p[0] not in spam_ids][:max_posts]

    print(f'Posts with < {max_comments} API comments (excluding spam): {len(post_data):,}')
    post_ids = [p[0] for p in post_data]

    print(f'Processing {len(post_ids):,} posts with stored comments...')

    results = []
    batch_size = 1000

    for batch_start in range(0, len(post_ids), batch_size):
        batch_ids = post_ids[batch_start:batch_start + batch_size]

        if batch_start % 5000 == 0:
            print(f'  Progress: {batch_start:,}/{len(post_ids):,}')

        # Load comments for this batch
        placeholders = ','.join('?' * len(batch_ids))
        comments = conn.execute(f'''
            SELECT id, post_id, parent_id
            FROM comments
            WHERE post_id IN ({placeholders})
        ''', batch_ids).fetchall()

        # Group by post
        comments_by_post = defaultdict(list)
        for cid, pid, parent in comments:
            comments_by_post[pid].append((cid, parent))

        # Process each post in batch
        for post_id in batch_ids:
            post_comments = comments_by_post.get(post_id, [])
            n = len(post_comments)
            if n < min_comments:
                continue

            # Build parent lookup
            comment_ids = {c[0] for c in post_comments}
            parent_map = {}
            for cid, parent in post_comments:
                if parent is not None and parent in comment_ids:
                    parent_map[cid] = parent

            # Compute depths iteratively
            # Depth 1 = direct reply to post (no parent or parent not in comment_ids)
            # Depth 2+ = replies to other comments
            depth_cache = {}
            for cid, _ in post_comments:
                if cid in depth_cache:
                    continue

                # Walk up chain
                chain = []
                current = cid
                while current is not None and current not in depth_cache:
                    chain.append(current)
                    current = parent_map.get(current)

                # Assign depths (direct replies have depth 1)
                if current is not None and current in depth_cache:
                    base = depth_cache[current]
                else:
                    base = 0  # Root will become depth 1

                for i, c in enumerate(reversed(chain)):
                    depth_cache[c] = base + i + 1

            depths = list(depth_cache.values())
            max_depth = max(depths) if depths else 1

            # Width = max comments at any depth level
            depth_counts = defaultdict(int)
            for d in depths:
                depth_counts[d] += 1
            max_width = max(depth_counts.values()) if depth_counts else 1

            results.append((post_id, n, max_depth, max_width))

    return results


def bin_and_average(x_vals, y_vals, num_bins=25, use_quantiles=False):
    """Bin x values logarithmically and compute average y per bin.

    If use_quantiles=True, returns (10th, 90th) percentiles instead of std error.
    """
    x_vals = np.array(x_vals)
    y_vals = np.array(y_vals)

    mask = (x_vals > 0) & (y_vals > 0)
    x_vals = x_vals[mask]
    y_vals = y_vals[mask]

    if len(x_vals) < 10:
        return np.array([]), np.array([]), np.array([]), np.array([])

    log_bins = np.logspace(np.log10(x_vals.min()), np.log10(x_vals.max()), num_bins)

    bin_centers = []
    bin_means = []
    bin_lo = []
    bin_hi = []

    for i in range(len(log_bins) - 1):
        mask = (x_vals >= log_bins[i]) & (x_vals < log_bins[i + 1])
        if mask.sum() >= 5:
            bin_centers.append(np.sqrt(log_bins[i] * log_bins[i + 1]))
            bin_means.append(np.mean(y_vals[mask]))
            if use_quantiles:
                bin_lo.append(np.percentile(y_vals[mask], 10))
                bin_hi.append(np.percentile(y_vals[mask], 90))
            else:
                std_err = np.std(y_vals[mask]) / np.sqrt(mask.sum())
                bin_lo.append(std_err)
                bin_hi.append(std_err)

    return np.array(bin_centers), np.array(bin_means), np.array(bin_lo), np.array(bin_hi)


def main():
    parser = argparse.ArgumentParser(description='Figure 4: Discussion Structure')
    parser.add_argument('--db', type=str, default=DEFAULT_DB)
    parser.add_argument('--output', type=str, default='figures/figure4_structure.pdf')
    parser.add_argument('--min-comments', type=int, default=5,
                        help='Minimum comments per post')
    parser.add_argument('--max-comments', type=int, default=100,
                        help='Max API-reported comments (for complete data)')
    parser.add_argument('--max-posts', type=int, default=30000,
                        help='Maximum posts to process')
    args = parser.parse_args()

    setup_style()
    conn = sqlite3.connect(args.db)

    # Get spam post IDs
    spam_ids = get_spam_post_ids(conn)
    print(f'Spam posts to exclude: {len(spam_ids):,}')

    # Compute tree metrics (only posts with complete comment data)
    print('Computing tree metrics...')
    metrics = compute_tree_metrics_batch(conn, spam_ids, args.min_comments, args.max_comments, args.max_posts)
    print(f'Posts with tree metrics: {len(metrics):,}')

    if len(metrics) == 0:
        print("No posts with sufficient comments found.")
        conn.close()
        return

    tree_sizes = np.array([m[1] for m in metrics])
    depths = np.array([m[2] for m in metrics])
    widths = np.array([m[3] for m in metrics])

    # Normalized metrics
    norm_depths = depths / np.sqrt(tree_sizes)
    norm_widths = widths / np.sqrt(tree_sizes)

    # Create figure
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    # Panel a: Normalized Depth vs Normalized Width scatter
    ax = axes[0]

    # Filter for plotting (need positive values for log scale)
    plot_mask = (norm_depths > 0) & (norm_widths > 0)
    nd_plot = norm_depths[plot_mask]
    nw_plot = norm_widths[plot_mask]

    # Bin and compute mean trend with 10-90 quantiles (fewer bins for cleaner plot)
    if len(nd_plot) > 0:
        x_bins, y_means, y_lo, y_hi = bin_and_average(nw_plot, nd_plot, num_bins=15, use_quantiles=True)
        if len(x_bins) > 0:
            # Plot with asymmetric error bars (10th to 90th percentile)
            yerr_lo = y_means - y_lo
            yerr_hi = y_hi - y_means
            ax.errorbar(x_bins, y_means, yerr=[yerr_lo, yerr_hi], fmt='o-', color=COLORS['blue'],
                        markersize=6, linewidth=1.5, capsize=2, alpha=0.8,
                        label='AI agents (10-90%)')

            # Fit power law
            exp, pref = fit_power_law(x_bins, y_means)
            if exp is not None:
                x_fit = np.logspace(np.log10(x_bins.min()), np.log10(x_bins.max()), 50)
                ax.loglog(x_fit, pref * x_fit**exp, '--', color=COLORS['grey'],
                          linewidth=2, label=f'd/√n ∝ (w/√n)^{exp:.2f}')

    ax.set_xlabel('Normalized width (w/√n)')
    ax.set_ylabel('Normalized depth (d/√n)')
    ax.legend(loc='upper left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'a')

    # Panel b: Normalized depth histogram (PDF) - linear scale
    ax = axes[1]

    # Create histogram with linear bins
    nd_positive = norm_depths[norm_depths > 0]
    if len(nd_positive) > 0:
        # Linear bins for histogram
        bins = np.linspace(0, min(nd_positive.max(), 3), 40)

        ax.hist(nd_positive, bins=bins, density=True, alpha=0.7,
                color=COLORS['green'], edgecolor='white', linewidth=0.5)

    ax.set_xlabel('Normalized depth (d/√n)')
    ax.set_ylabel('PDF P(d/√n)')
    ax.set_xlim(0, 3)
    add_panel_label(ax, 'b')

    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f'\nSaved: {args.output}')

    # Print summary stats
    print(f'\n=== Summary ===')
    print(f'Tree sizes: median={np.median(tree_sizes):.0f}, max={np.max(tree_sizes):.0f}')
    print(f'Depths: median={np.median(depths):.1f}, max={np.max(depths):.0f}')
    print(f'Widths: median={np.median(widths):.1f}, max={np.max(widths):.0f}')
    print(f'Normalized depth: median={np.median(norm_depths):.3f}, mean={np.mean(norm_depths):.3f}')
    print(f'Normalized width: median={np.median(norm_widths):.3f}, mean={np.mean(norm_widths):.3f}')
    print(f'Posts with depth=1 only: {np.sum(depths == 1):,} ({100*np.mean(depths == 1):.1f}%)')

    conn.close()


if __name__ == "__main__":
    main()
