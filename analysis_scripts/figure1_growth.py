#!/usr/bin/env python3
"""
Figure 1: Platform Growth

Two-panel figure showing:
a) Cumulative counts of posts, comments, and agents over time
b) Daily new posts, comments, and agents
"""
import sqlite3
import argparse
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from collections import defaultdict

from figure_style import (
    setup_style, COLORS, add_panel_label, DEFAULT_DB
)

DEFAULT_DB = "data/moltbook_paper.db"


def parse_ts(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
    except:
        return None


def load_temporal_data(conn):
    """Load temporal data for posts, comments, and agents at daily resolution."""
    # Posts by day
    posts = conn.execute('''
        SELECT DATE(created_at) as day, COUNT(*) as count
        FROM posts
        WHERE created_at IS NOT NULL
        GROUP BY day
        ORDER BY day
    ''').fetchall()
    posts_by_day = {r[0]: r[1] for r in posts}

    # Comments by day (actual stored comments)
    comments = conn.execute('''
        SELECT DATE(created_at) as day, COUNT(*) as count
        FROM comments
        WHERE created_at IS NOT NULL
        GROUP BY day
        ORDER BY day
    ''').fetchall()
    comments_by_day = {r[0]: r[1] for r in comments}

    # Active agents by day (first post or comment)
    agent_first_post = conn.execute('''
        SELECT author_id, MIN(DATE(created_at)) as first_day
        FROM posts
        WHERE created_at IS NOT NULL AND author_id IS NOT NULL
        GROUP BY author_id
    ''').fetchall()

    agent_first_comment = conn.execute('''
        SELECT author_id, MIN(DATE(created_at)) as first_day
        FROM comments
        WHERE created_at IS NOT NULL AND author_id IS NOT NULL
        GROUP BY author_id
    ''').fetchall()

    # Combine to get first activity day per agent
    agent_first = {}
    for aid, day in agent_first_post:
        agent_first[aid] = day
    for aid, day in agent_first_comment:
        if aid not in agent_first or day < agent_first[aid]:
            agent_first[aid] = day

    # Count new agents per day
    agents_by_day = defaultdict(int)
    for aid, day in agent_first.items():
        agents_by_day[day] += 1

    # API-reported comments by day (from post_snapshots)
    # Only include days where we have >90% post coverage
    api_comments_by_day = {}
    try:
        api_comments = conn.execute('''
            WITH daily_max AS (
                SELECT post_id, DATE(recorded_at) as day, MAX(comment_count) as max_count
                FROM post_snapshots
                GROUP BY post_id, DATE(recorded_at)
            ),
            daily_totals AS (
                SELECT day, SUM(max_count) as total, COUNT(DISTINCT post_id) as posts_covered
                FROM daily_max
                GROUP BY day
            ),
            cumulative_posts AS (
                SELECT DATE(created_at) as day,
                       SUM(COUNT(*)) OVER (ORDER BY DATE(created_at)) as cum_posts
                FROM posts
                WHERE created_at IS NOT NULL
                GROUP BY DATE(created_at)
            )
            SELECT d.day, d.total
            FROM daily_totals d
            JOIN cumulative_posts c ON d.day = c.day
            WHERE d.posts_covered * 1.0 / c.cum_posts > 0.9
            ORDER BY d.day
        ''').fetchall()
        api_comments_by_day = {r[0]: r[1] for r in api_comments}
    except:
        pass  # Table might not exist

    return posts_by_day, comments_by_day, dict(agents_by_day), api_comments_by_day


def main():
    parser = argparse.ArgumentParser(description='Figure 1: Platform Growth')
    parser.add_argument('--db', type=str, default=DEFAULT_DB)
    parser.add_argument('--output', type=str, default='figures/figure1_growth.pdf')
    args = parser.parse_args()

    setup_style()
    conn = sqlite3.connect(args.db)

    posts_by_day, comments_by_day, agents_by_day, api_comments_by_day = load_temporal_data(conn)

    # Get all days
    all_days = sorted(set(posts_by_day.keys()) |
                      set(comments_by_day.keys()) |
                      set(agents_by_day.keys()))

    dates = np.array([datetime.strptime(d, '%Y-%m-%d') for d in all_days])
    posts_daily = np.array([posts_by_day.get(d, 0) for d in all_days], dtype=float)
    comments_daily = np.array([comments_by_day.get(d, 0) for d in all_days], dtype=float)
    agents_daily = np.array([agents_by_day.get(d, 0) for d in all_days], dtype=float)

    # For daily plot: replace zeros with NaN to create gaps instead of vertical lines
    posts_daily_plot = np.where(posts_daily > 0, posts_daily, np.nan)
    comments_daily_plot = np.where(comments_daily > 0, comments_daily, np.nan)
    agents_daily_plot = np.where(agents_daily > 0, agents_daily, np.nan)

    # Cumulative (continuous lines, but skip initial zeros)
    posts_cum = np.cumsum(posts_daily)
    comments_cum = np.cumsum(comments_daily)
    agents_cum = np.cumsum(agents_daily)

    # Replace leading zeros with NaN so lines start from first non-zero value
    posts_cum = np.where(posts_cum > 0, posts_cum, np.nan)
    comments_cum = np.where(comments_cum > 0, comments_cum, np.nan)
    agents_cum = np.where(agents_cum > 0, agents_cum, np.nan)

    # API comments (from snapshots, only available for some days)
    # This is cumulative totals from snapshots
    api_comments_cum = np.array([api_comments_by_day.get(d, 0) for d in all_days], dtype=float)
    api_comments_cum_plot = np.where(api_comments_cum > 0, api_comments_cum, np.nan)

    # Compute daily API comments as difference between consecutive days
    api_comments_daily = np.zeros_like(api_comments_cum)
    for i in range(1, len(api_comments_cum)):
        if api_comments_cum[i] > 0 and api_comments_cum[i-1] > 0:
            api_comments_daily[i] = api_comments_cum[i] - api_comments_cum[i-1]
    api_comments_daily_plot = np.where(api_comments_daily > 0, api_comments_daily, np.nan)

    # Create figure
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    # Set up date ticks every 2 days, starting from an even day
    start_date = dates[0]
    end_date = dates[-1]
    # Round start to nearest even day
    tick_start = start_date - timedelta(days=start_date.day % 2)
    tick_dates = []
    current = tick_start
    while current <= end_date + timedelta(days=1):
        if current >= start_date:
            tick_dates.append(current)
        current += timedelta(days=2)

    # Panel a: Cumulative counts (log y-scale)
    ax = axes[0]
    ax.semilogy(dates, comments_cum, '-', color=COLORS['blue'], linewidth=2, label='Comments (stored)')
    ax.semilogy(dates, posts_cum, '-', color=COLORS['orange'], linewidth=2, label='Posts')
    ax.semilogy(dates, agents_cum, '-', color=COLORS['green'], linewidth=2, label='Active agents')

    # API comments (dashed line, only where data exists)
    ax.semilogy(dates, api_comments_cum_plot, '--', color=COLORS['blue'], linewidth=2,
                alpha=0.7, label='Comments (API)')

    ax.set_xlabel('Date')
    ax.set_ylabel('Cumulative count')
    ax.legend(loc='lower right', frameon=True, fontsize=8)
    ax.set_xticks(tick_dates)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, which='both')

    add_panel_label(ax, 'a')

    # Panel b: Daily counts (log y-scale)
    ax = axes[1]
    ax.semilogy(dates, comments_daily_plot, '-', color=COLORS['blue'], linewidth=2, alpha=0.8, label='Comments (stored)')
    ax.semilogy(dates, posts_daily_plot, '-', color=COLORS['orange'], linewidth=2, alpha=0.8, label='Posts')
    ax.semilogy(dates, agents_daily_plot, '-', color=COLORS['green'], linewidth=2, alpha=0.8, label='New agents')

    ax.set_xlabel('Date')
    ax.set_ylabel('Daily count')
    ax.legend(loc='lower right', frameon=True, fontsize=8)
    ax.set_xticks(tick_dates)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, which='both')

    add_panel_label(ax, 'b')

    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f'Saved: {args.output}')

    # Print summary stats
    print(f'\n=== Summary ===')
    print(f'Total posts: {int(posts_cum[-1]):,}')
    print(f'Total comments: {int(comments_cum[-1]):,}')
    print(f'Total active agents: {int(agents_cum[-1]):,}')
    print(f'Time range: {all_days[0]} to {all_days[-1]}')

    conn.close()


if __name__ == "__main__":
    main()
