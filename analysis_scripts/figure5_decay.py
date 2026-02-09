#!/usr/bin/env python3
"""
Figure 5: Temporal Dynamics

Two-panel figure showing:
a) Decay factor γ(t) - mean growth ratio over time (log-log)
b) Duration distribution: PDF using cohort approach (Feb 2)

Replicates Asur et al. (2011) Figures 2-3 analysis.

Note: Only uses posts with < 100 API-reported comments to ensure complete
comment data (API returns max 100 comments per post).
"""
import sqlite3
import argparse
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from collections import defaultdict

from figure_style import (
    setup_style, COLORS, COLOR_SEQUENCE, add_panel_label, format_axis_log,
    compute_pdf_logbins, fit_power_law, get_spam_post_ids
)

DEFAULT_DB = "data/moltbook_paper.db"


def parse_ts(ts):
    """Parse timestamp string to datetime."""
    if ts is None:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
    except:
        return None


def load_comment_timeseries(conn, spam_ids, min_comments=5, max_comments=100):
    """
    Load comment arrival times for each post.
    Only includes posts with < max_comments (API-reported) to ensure complete data.
    Returns dict: post_id -> list of relative hours since post creation.
    """
    # Get post creation times (only posts with complete comment data)
    post_rows = conn.execute('''
        SELECT id, created_at, comment_count FROM posts
        WHERE created_at IS NOT NULL
        AND comment_count IS NOT NULL
        AND comment_count > 0
        AND comment_count < ?
    ''', (max_comments,)).fetchall()

    post_created = {}
    post_api_count = {}
    for pid, ts, api_count in post_rows:
        if pid in spam_ids:
            continue
        dt = parse_ts(ts)
        if dt:
            post_created[pid] = dt
            post_api_count[pid] = api_count

    print(f'Posts with < {max_comments} API comments (excluding spam): {len(post_created):,}')

    # Get comments with timestamps
    comment_rows = conn.execute('''
        SELECT post_id, created_at
        FROM comments
        WHERE created_at IS NOT NULL
        ORDER BY post_id, created_at
    ''').fetchall()

    # Build timeseries for each post
    post_comments = defaultdict(list)
    for post_id, ts in comment_rows:
        if post_id not in post_created:
            continue
        cdt = parse_ts(ts)
        if cdt is None:
            continue
        rel_hours = (cdt - post_created[post_id]).total_seconds() / 3600.0
        if rel_hours >= 0:
            post_comments[post_id].append(rel_hours)

    # Filter by minimum comments
    filtered = {pid: times for pid, times in post_comments.items()
                if len(times) >= min_comments}

    return filtered, post_created


def compute_cumulative_counts(post_comments, time_bins):
    """Compute cumulative comment counts N(t) for each post at given time points."""
    cumulative = {}
    for post_id, times in post_comments.items():
        times_arr = np.array(times)
        counts = np.array([np.sum(times_arr <= t) for t in time_bins])
        cumulative[post_id] = counts
    return cumulative


def compute_decay_factor(post_comments, time_bins):
    """
    Compute decay factor γ(t) = mean(N(t)/N(t-1)) - 1 for each time step.
    """
    cumulative = compute_cumulative_counts(post_comments, time_bins)

    gamma_data = []
    for t_idx in range(1, len(time_bins)):
        ratios = []
        for post_id, counts in cumulative.items():
            if counts[t_idx - 1] > 0:
                ratio = counts[t_idx] / counts[t_idx - 1]
                ratios.append(ratio)

        if len(ratios) > 100:
            gamma = np.mean(ratios) - 1
            if gamma > 0:
                gamma_data.append((time_bins[t_idx], gamma))

    return gamma_data


def compute_durations(post_comments):
    """Compute activity duration for each post (time from post creation to last comment)."""
    durations = []
    for post_id, times in post_comments.items():
        if len(times) < 1:
            continue
        duration = max(times)  # times are relative to post creation
        if duration > 0:
            durations.append(duration)
    return np.array(durations)


def main():
    parser = argparse.ArgumentParser(description='Figure 5: Temporal Dynamics')
    parser.add_argument('--db', type=str, default=DEFAULT_DB)
    parser.add_argument('--output', type=str, default='figures/figure5_decay.pdf')
    parser.add_argument('--min-comments', type=int, default=5)
    parser.add_argument('--max-comments', type=int, default=100,
                        help='Max API-reported comments (for complete data)')
    args = parser.parse_args()

    setup_style()
    conn = sqlite3.connect(args.db)

    # Get spam post IDs
    spam_ids = get_spam_post_ids(conn)
    print(f'Spam posts to exclude: {len(spam_ids):,}')

    # Load comment timeseries (only posts with complete data)
    print('Loading comment timeseries...')
    post_comments, post_created = load_comment_timeseries(
        conn, spam_ids, args.min_comments, args.max_comments)
    print(f'Posts with {args.min_comments} to <{args.max_comments} comments: {len(post_comments):,}')

    total_comments = sum(len(c) for c in post_comments.values())
    print(f'Total comments in analysis: {total_comments:,}')

    # Create figure
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    # Panel a: Decay factor γ(t)
    ax = axes[0]

    # Use fine time bins (30-min intervals up to 48 hours)
    time_bins = np.linspace(0.5, 48, 96)
    gamma_data = compute_decay_factor(post_comments, time_bins)

    if len(gamma_data) > 0:
        times = np.array([g[0] for g in gamma_data])
        gammas = np.array([g[1] for g in gamma_data])

        ax.loglog(times, gammas, 'o', color=COLORS['blue'], markersize=4, alpha=0.7)

        # Fit power law
        fit_mask = (times >= 2) & (times <= 30)
        if fit_mask.sum() > 5:
            exp, pref = fit_power_law(times[fit_mask], gammas[fit_mask])
            if exp is not None:
                t_fit = np.logspace(np.log10(times.min()), np.log10(times.max()), 100)
                g_fit = pref * t_fit**exp
                ax.loglog(t_fit, g_fit, '--', color=COLORS['grey'], linewidth=2,
                          label=f'γ(t) ∝ t^{exp:.2f}')
                print(f'Decay exponent: {exp:.2f} (Asur found -1.0)')

    ax.set_xlabel('Time since post creation (hours)')
    ax.set_ylabel('Decay factor γ(t)')
    ax.set_xlim(0.4, 60)
    ax.legend(loc='upper right', fontsize=9)
    format_axis_log(ax)
    add_panel_label(ax, 'a')

    # Panel b: Duration distribution using cohort approach (3 cohorts from Feb 2)
    ax = axes[1]

    cohort_date = datetime(2026, 2, 2)
    cohort_colors = [COLORS['blue'], COLORS['orange'], COLORS['green']]
    cohort_labels = ['00:00-08:00', '08:00-16:00', '16:00-24:00']

    all_cohort_durations = []

    for i in range(3):
        cohort_start = cohort_date + timedelta(hours=i * 8)
        cohort_end = cohort_start + timedelta(hours=8)

        # Find posts created in this cohort window
        cohort_durations = []
        for pid, created_dt in post_created.items():
            if pid not in post_comments:
                continue
            if cohort_start <= created_dt < cohort_end:
                times = post_comments[pid]
                if len(times) >= 1:
                    duration = max(times)  # time from post creation to last comment
                    if duration > 0:
                        cohort_durations.append(duration)

        cohort_durations = np.array(cohort_durations)
        all_cohort_durations.extend(cohort_durations)

        if len(cohort_durations) > 50:
            bin_centers, pdf = compute_pdf_logbins(cohort_durations, num_bins=25)
            if len(bin_centers) > 0:
                ax.loglog(bin_centers, pdf, 'o-', color=cohort_colors[i],
                          markersize=4, linewidth=1.5, alpha=0.8,
                          label=f'{cohort_labels[i]} (n={len(cohort_durations):,})')

        print(f'Cohort Feb 2 {cohort_labels[i]}: {len(cohort_durations):,} posts')

    # Fit power law to combined cohort data
    all_cohort_durations = np.array(all_cohort_durations)
    if len(all_cohort_durations) > 0:
        bin_centers, pdf = compute_pdf_logbins(all_cohort_durations, num_bins=35)
        fit_mask = (bin_centers >= 0.04) & (bin_centers <= 10)
        if fit_mask.sum() >= 5:
            exp, pref = fit_power_law(bin_centers[fit_mask], pdf[fit_mask])
            if exp is not None:
                x_fit = np.logspace(-1.5, 2, 50)
                ax.loglog(x_fit, pref * x_fit**exp, '--', color=COLORS['grey'],
                          linewidth=2, label=f'α = {exp:.2f}')
                print(f'Cohort duration exponent: {exp:.2f}')

        print(f'Cohort median duration: {np.median(all_cohort_durations):.1f} hours')

    ax.set_xlabel('Activity duration (hours)')
    ax.set_ylabel('PDF P(duration)')
    ax.legend(loc='upper right', fontsize=8)
    format_axis_log(ax)
    add_panel_label(ax, 'b')

    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f'\nSaved: {args.output}')

    conn.close()


if __name__ == "__main__":
    main()
