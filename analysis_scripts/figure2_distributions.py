#!/usr/bin/env python3
"""
Figure 2: Distribution of Quantities

Three-panel figure showing CCDFs:
a) Number of comments per post (spam-filtered)
b) Number of posts per submolt (with featured submolts marked)
c) Number of subscribers per submolt (with featured submolts marked)

Uses powerlaw package for proper power-law fitting with statistical tests.
"""
import sqlite3
import argparse
import numpy as np
import matplotlib.pyplot as plt
import powerlaw

from figure_style import (
    setup_style, COLORS, COLOR_SEQUENCE, FEATURED_SUBMOLTS,
    add_panel_label, format_axis_log, compute_ccdf,
    get_spam_post_ids
)

DEFAULT_DB = "data/moltbook_paper.db"


def get_comment_counts_filtered(conn, spam_ids):
    """Get comment counts per post (API-reported), excluding spam-flooded posts."""
    rows = conn.execute('''
        SELECT id, comment_count
        FROM posts
        WHERE comment_count IS NOT NULL AND comment_count > 0
    ''').fetchall()

    counts = [r[1] for r in rows if r[0] not in spam_ids]
    return np.array(counts)


def get_posts_per_submolt(conn):
    """Get number of posts per submolt with names."""
    rows = conn.execute('''
        SELECT s.name, COUNT(p.id) as post_count
        FROM submolts s
        LEFT JOIN posts p ON p.submolt_id = s.id
        GROUP BY s.id
        HAVING post_count > 0
        ORDER BY post_count DESC
    ''').fetchall()
    return [(r[0], r[1]) for r in rows]


def get_subscribers_per_submolt(conn):
    """Get subscriber count per submolt with names."""
    # Check if submolt_snapshots exists
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]

    if 'submolt_snapshots' in tables:
        rows = conn.execute('''
            SELECT s.name, MAX(ss.subscriber_count) as subs
            FROM submolts s
            JOIN submolt_snapshots ss ON s.id = ss.submolt_id
            GROUP BY s.id
            HAVING subs > 0
            ORDER BY subs DESC
        ''').fetchall()
    else:
        rows = conn.execute('''
            SELECT name, subscriber_count
            FROM submolts
            WHERE subscriber_count > 0
            ORDER BY subscriber_count DESC
        ''').fetchall()

    return [(r[0], r[1]) for r in rows]


def fit_powerlaw_with_test(data, xmin=None):
    """
    Fit power law using powerlaw package and compare to lognormal.
    Returns: (alpha, xmin, R, p) where R is log-likelihood ratio and p is p-value.
    """
    data = np.array(data)
    data = data[data > 0]

    fit = powerlaw.Fit(data, xmin=xmin, discrete=True, verbose=False)

    # Compare to lognormal
    try:
        R, p = fit.distribution_compare('power_law', 'lognormal', normalized_ratio=True)
    except:
        R, p = np.nan, np.nan

    return fit.power_law.alpha, fit.power_law.xmin, R, p


def main():
    parser = argparse.ArgumentParser(description='Figure 2: Distributions')
    parser.add_argument('--db', type=str, default=DEFAULT_DB)
    parser.add_argument('--output', type=str, default='figures/figure2_distributions.pdf')
    args = parser.parse_args()

    setup_style()
    conn = sqlite3.connect(args.db)

    # Get spam post IDs
    spam_ids = get_spam_post_ids(conn)
    print(f'Spam posts to exclude: {len(spam_ids):,}')

    # Load data
    comment_counts = get_comment_counts_filtered(conn, spam_ids)
    posts_per_submolt = get_posts_per_submolt(conn)
    subs_per_submolt = get_subscribers_per_submolt(conn)

    print(f'Posts (filtered): {len(comment_counts):,}')
    print(f'Submolts with posts: {len(posts_per_submolt):,}')
    print(f'Submolts with subscribers: {len(subs_per_submolt):,}')

    # Create figure
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))

    # Panel a: Comments per post CCDF
    ax = axes[0]
    x, y = compute_ccdf(comment_counts)
    ax.loglog(x, y, '-', color=COLORS['blue'], linewidth=1.5, alpha=0.8)

    # Fit power law with powerlaw package (force xmin=100 for tail fit)
    try:
        alpha, xmin, R, p = fit_powerlaw_with_test(comment_counts, xmin=100)
        print(f'\n=== Comments per post ===')
        print(f'  α = {alpha:.2f}, xmin = {xmin:.0f}')
        print(f'  Power law vs lognormal: R = {R:.2f}, p = {p:.3f}')

        # Plot fitted power law manually with good positioning
        x_fit = np.logspace(2, 5.5, 50)  # 100 to ~300k
        y_fit = 0.01 * (x_fit / 100) ** (-(alpha - 1))
        ax.loglog(x_fit, y_fit, '--', color=COLORS['grey'],
                  linewidth=1.5, label=f'α = {alpha:.2f}')
    except Exception as e:
        print(f'Power law fit failed: {e}')

    ax.set_xlabel('Comments per post')
    ax.set_ylabel('CCDF P(X ≥ x)')
    ax.legend(loc='lower left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'a')

    # Panel b: Posts per submolt CCDF
    ax = axes[1]
    submolt_names_posts = [r[0] for r in posts_per_submolt]
    post_counts = np.array([r[1] for r in posts_per_submolt])

    # Plot CCDF for ALL submolts (not excluding featured)
    x, y = compute_ccdf(post_counts)
    ax.loglog(x, y, '-', color=COLORS['orange'], linewidth=1.5, alpha=0.8)

    # Fit power law
    try:
        alpha, xmin, R, p = fit_powerlaw_with_test(post_counts)
        print(f'\n=== Posts per submolt ===')
        print(f'  α = {alpha:.2f}, xmin = {xmin:.0f}')
        print(f'  Power law vs lognormal: R = {R:.2f}, p = {p:.3f}')

        # Plot fitted power law manually with good positioning
        x_fit = np.logspace(2, 5, 50)  # 10 to 10^4
        y_fit = 0.8 * (x_fit / 10) ** (-(alpha - 1))
        ax.loglog(x_fit, y_fit, '--', color=COLORS['grey'],
                  linewidth=1.5, label=f'α = {alpha:.2f}')
    except Exception as e:
        print(f'Power law fit failed: {e}')

    # Mark featured submolts ON the CCDF
    for name, count in zip(submolt_names_posts, post_counts):
        if name in FEATURED_SUBMOLTS:
            rank = np.sum(post_counts >= count)
            ccdf_val = rank / len(post_counts)
            ax.plot(count, ccdf_val, 'o', color=COLORS['red'], markersize=8,
                    markeredgecolor='white', markeredgewidth=0.5, zorder=5)

    ax.set_xlabel('Posts per submolt')
    ax.set_ylabel('CCDF P(X ≥ x)')
    ax.legend(loc='lower left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'b')

    # Panel c: Subscribers per submolt CCDF
    ax = axes[2]
    submolt_names_subs = [r[0] for r in subs_per_submolt]
    sub_counts = np.array([r[1] for r in subs_per_submolt])

    # Plot CCDF for ALL submolts
    x, y = compute_ccdf(sub_counts)
    ax.loglog(x, y, '-', color=COLORS['green'], linewidth=1.5, alpha=0.8)

    # Fit power law
    try:
        alpha, xmin, R, p = fit_powerlaw_with_test(sub_counts)
        print(f'\n=== Subscribers per submolt ===')
        print(f'  α = {alpha:.2f}, xmin = {xmin:.0f}')
        print(f'  Power law vs lognormal: R = {R:.2f}, p = {p:.3f}')

        # Plot fitted power law manually with good positioning
        x_fit = np.logspace(1, 3, 50)  # 1 to 1000
        y_fit = 2 * (x_fit / 1) ** (-(alpha - 1))
        ax.loglog(x_fit, y_fit, '--', color=COLORS['grey'],
                  linewidth=1.5, label=f'α = {alpha:.2f}')
    except Exception as e:
        print(f'Power law fit failed: {e}')

    # Mark featured submolts
    for name, count in zip(submolt_names_subs, sub_counts):
        if name in FEATURED_SUBMOLTS:
            rank = np.sum(sub_counts >= count)
            ccdf_val = rank / len(sub_counts)
            ax.plot(count, ccdf_val, 'o', color=COLORS['red'], markersize=8,
                    markeredgecolor='white', markeredgewidth=0.5, zorder=5)

    ax.set_xlabel('Subscribers per submolt')
    ax.set_ylabel('CCDF P(X ≥ x)')
    ax.legend(loc='lower left', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'c')

    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f'\nSaved: {args.output}')

    conn.close()


if __name__ == "__main__":
    main()
