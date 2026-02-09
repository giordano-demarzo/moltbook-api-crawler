#!/usr/bin/env python3
"""
Continuous DB maintenance daemon for Moltbook.

Three phases per cycle:
  1. Fast full listing scan — record snapshots, detect comment-count changes
  2. Targeted detail fetch — fetch /posts/{id} only for changed posts
  3. Profile + submolt sweep — refresh stale agents & all submolts

Usage:
  python maintain_db.py                          # all 3 phases, once
  python maintain_db.py --loop --interval 3600   # daemon: every hour
  python maintain_db.py --phase 1                # listing scan only
  python maintain_db.py --phase 1 --phase 3      # listing + sweeps
  python maintain_db.py -v                       # verbose (DEBUG)
"""
import argparse
import asyncio
import random
import signal
import sqlite3
import os
import sys
import time
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Set, Tuple

import aiohttp


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
API_BASE = "https://www.moltbook.com/api/v1"
DEFAULT_DB = "data/moltbook.db"
HTTP_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}

logger = logging.getLogger("maintain_db")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    logger.setLevel(level)

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(fmt))
    logger.addHandler(console)

    # Rotating file
    os.makedirs("logs", exist_ok=True)
    fh = RotatingFileHandler(
        "logs/maintain_db.log", maxBytes=50 * 1024 * 1024, backupCount=5
    )
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def init_db(db_path: str):
    """Ensure all tables + new snapshot tables exist; enable WAL."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE,
            description TEXT,
            karma INTEGER DEFAULT 0,
            follower_count INTEGER DEFAULT 0,
            following_count INTEGER DEFAULT 0,
            x_handle TEXT,
            x_name TEXT,
            x_bio TEXT,
            x_follower_count INTEGER DEFAULT 0,
            x_verified INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            url TEXT,
            submolt_id TEXT,
            submolt TEXT,
            submolt_display TEXT,
            author_id TEXT,
            author_name TEXT,
            upvotes INTEGER DEFAULT 0,
            downvotes INTEGER DEFAULT 0,
            comment_count INTEGER DEFAULT 0,
            created_at TIMESTAMP,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (author_id) REFERENCES agents(id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            parent_id TEXT,
            content TEXT,
            author_id TEXT,
            author_name TEXT,
            upvotes INTEGER DEFAULT 0,
            downvotes INTEGER DEFAULT 0,
            depth INTEGER DEFAULT 0,
            created_at TIMESTAMP,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (parent_id) REFERENCES comments(id),
            FOREIGN KEY (author_id) REFERENCES agents(id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS submolts (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE,
            display_name TEXT,
            description TEXT,
            subscriber_count INTEGER DEFAULT 0,
            created_at TIMESTAMP,
            last_activity_at TIMESTAMP,
            featured_at TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS post_snapshots (
            post_id TEXT NOT NULL,
            upvotes INTEGER,
            downvotes INTEGER,
            comment_count INTEGER,
            recorded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (post_id, recorded_at)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS agent_snapshots (
            agent_id TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            karma INTEGER,
            follower_count INTEGER,
            following_count INTEGER,
            PRIMARY KEY (agent_id, recorded_at)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS submolt_snapshots (
            submolt_id TEXT NOT NULL,
            recorded_at TIMESTAMP NOT NULL,
            subscriber_count INTEGER,
            PRIMARY KEY (submolt_id, recorded_at)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS homepage_stats (
            recorded_at TIMESTAMP PRIMARY KEY,
            agents INTEGER,
            submolts INTEGER,
            posts INTEGER,
            comments INTEGER
        )
    ''')

    # Indexes
    cur.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_post ON post_snapshots(post_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_time ON post_snapshots(recorded_at)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_agent_snap_agent ON agent_snapshots(agent_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_agent_snap_time ON agent_snapshots(recorded_at)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_submolt_snap_id ON submolt_snapshots(submolt_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_submolt_snap_time ON submolt_snapshots(recorded_at)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_submolt ON posts(submolt)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_name)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_homepage_stats_time ON homepage_stats(recorded_at)')

    conn.commit()
    conn.close()
    logger.info("Database initialized: %s", db_path)


def get_stored_comment_counts(db_path: str) -> Dict[str, int]:
    """Return {post_id: actual_saved_comments} for every post in DB.

    Uses the real count from the comments table, NOT posts.comment_count,
    so that posts whose comments were never fetched get flagged.
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute('''
        SELECT p.id, COUNT(c.id)
        FROM posts p
        LEFT JOIN comments c ON c.post_id = p.id
        GROUP BY p.id
    ''').fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# Comment / agent helpers (copied from smart_crawler.py, self-contained)
# ---------------------------------------------------------------------------
def flatten_comments(comments: List[Dict], post_id: str,
                     parent_id: str = None, depth: int = 0) -> List[Dict]:
    flat: List[Dict] = []
    for c in comments:
        c["_post_id"] = post_id
        c["_parent_id"] = parent_id
        c["_depth"] = depth
        flat.append(c)
        replies = c.get("replies", [])
        if replies:
            flat.extend(flatten_comments(replies, post_id, c.get("id"), depth + 1))
    return flat


def save_agent_full(cursor: sqlite3.Cursor, agent: Dict):
    if not agent or not agent.get("id"):
        return
    owner = agent.get("owner", {}) or {}
    cursor.execute('''
        INSERT OR REPLACE INTO agents
        (id, name, description, karma, follower_count, following_count,
         x_handle, x_name, x_bio, x_follower_count, x_verified, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        agent.get("id"),
        agent.get("name"),
        agent.get("description"),
        agent.get("karma", 0),
        agent.get("follower_count", 0),
        agent.get("following_count", 0),
        owner.get("x_handle"),
        owner.get("x_name"),
        owner.get("x_bio"),
        owner.get("x_follower_count", 0),
        1 if owner.get("x_verified") else 0,
    ))


# ---------------------------------------------------------------------------
# HTTP fetchers (with retry / back-off)
# ---------------------------------------------------------------------------
RATE_LIMITED = {"_rate_limited": True}


async def fetch_posts_page(session: aiohttp.ClientSession, offset: int,
                           limit: int = 50, sort: str = "new",
                           retries: int = 10) -> Optional[Dict]:
    limit = min(limit, 50)
    url = f"{API_BASE}/posts?limit={limit}&offset={offset}&sort={sort}"
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    return RATE_LIMITED
                if resp.status >= 500:
                    logger.warning("HTTP %d at offset %d (attempt %d/%d)",
                                   resp.status, offset, attempt, retries)
                else:
                    logger.error("HTTP %d fetching posts offset %d", resp.status, offset)
                    return {"posts": [], "has_more": False}
        except Exception as exc:
            logger.warning("Exception fetching posts offset %d: %s (attempt %d/%d)",
                           offset, exc, attempt, retries)
        if attempt < retries:
            await asyncio.sleep(1.0 * attempt)
    logger.error("Failed to fetch posts after %d attempts (offset %d)", retries, offset)
    return None


async def fetch_post_details(session: aiohttp.ClientSession, post_id: str,
                             semaphore: asyncio.Semaphore,
                             retries: int = 5) -> Optional[Dict]:
    async with semaphore:
        for attempt in range(1, retries + 1):
            url = f"{API_BASE}/posts/{post_id}"
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 429:
                        delay = 2.0 * attempt + random.uniform(0, 2)
                        if attempt < retries:
                            await asyncio.sleep(delay)
                        continue
                    if resp.status >= 500:
                        logger.debug("HTTP %d for post %s (attempt %d/%d)",
                                     resp.status, post_id, attempt, retries)
                    else:
                        logger.warning("HTTP %d for post %s (attempt %d/%d)",
                                       resp.status, post_id, attempt, retries)
            except Exception as exc:
                logger.warning("Exception fetching post %s: %s (attempt %d/%d)",
                               post_id, exc, attempt, retries)
            if attempt < retries:
                await asyncio.sleep(1.0 * attempt)
    return None


async def fetch_homepage_stats(session: aiohttp.ClientSession,
                               retries: int = 5) -> Optional[Dict]:
    """Fetch homepage stats (agents, submolts, posts, comments counts)."""
    url = f"{API_BASE}/homepage?shuffle={int(time.time() * 1000)}"
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and "stats" in data:
                        return data["stats"]
                if resp.status == 429:
                    delay = 2.0 * attempt + random.uniform(0, 2)
                    if attempt < retries:
                        await asyncio.sleep(delay)
                    continue
                if resp.status >= 500:
                    logger.warning("HTTP %d fetching homepage (attempt %d/%d)",
                                   resp.status, attempt, retries)
        except Exception as exc:
            logger.warning("Exception fetching homepage: %s (attempt %d/%d)",
                           exc, attempt, retries)
        if attempt < retries:
            await asyncio.sleep(1.0 * attempt)
    return None


async def fetch_submolts_page(session: aiohttp.ClientSession, offset: int,
                              limit: int = 100, retries: int = 5) -> Dict:
    url = f"{API_BASE}/submolts?limit={limit}&offset={offset}"
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    delay = 2.0 * attempt + random.uniform(0, 2)
                    if attempt < retries:
                        await asyncio.sleep(delay)
                    continue
                if resp.status >= 500:
                    logger.warning("HTTP %d fetching submolts offset %d (attempt %d/%d)",
                                   resp.status, offset, attempt, retries)
                else:
                    logger.error("HTTP %d fetching submolts offset %d", resp.status, offset)
                    return {"submolts": [], "count": 0}
        except Exception as exc:
            logger.warning("Exception fetching submolts offset %d: %s (attempt %d/%d)",
                           offset, exc, attempt, retries)
        if attempt < retries:
            await asyncio.sleep(1.0 * attempt)
    logger.error("Failed to fetch submolts after %d attempts (offset %d)", retries, offset)
    return {"submolts": [], "count": 0}


# ---------------------------------------------------------------------------
# Phase 1 — Fast full listing scan
# ---------------------------------------------------------------------------
async def _new_session() -> aiohttp.ClientSession:
    """Create a fresh HTTP session."""
    return aiohttp.ClientSession(headers=HTTP_HEADERS)


async def phase1_listing_scan(
    db_path: str,
    crawl_ts: str,
    concurrency: int = 5,
    shutdown: asyncio.Event = None,
) -> List[str]:
    """
    Scan all /posts?sort=new pages sequentially.
    Returns list of post IDs whose comment_count increased (or new with comments).
    """
    logger.info("=== Phase 1: Full listing scan (concurrency=%d) ===", concurrency)
    stored_counts = get_stored_comment_counts(db_path)
    logger.info("Loaded %d stored comment counts", len(stored_counts))

    changed_ids: List[str] = []
    snapshots: List[Tuple] = []
    post_rows: List[Tuple] = []
    agent_rows: List[Tuple] = []
    submolt_rows: List[Tuple] = []

    total_seen = 0
    total_new = 0
    total_updated = 0

    page_size = 50
    offset = 0
    done = False

    def _process_posts(posts: List[Dict]):
        nonlocal total_seen, total_new, total_updated
        for post in posts:
            pid = post.get("id")
            if not pid:
                continue
            total_seen += 1

            upvotes = post.get("upvotes", 0)
            downvotes = post.get("downvotes", 0)
            cc = post.get("comment_count", 0)

            snapshots.append((pid, upvotes, downvotes, cc, crawl_ts))

            submolt = post.get("submolt", {}) or {}
            author = post.get("author", {}) or {}
            post_rows.append((
                pid,
                post.get("title"),
                post.get("content"),
                post.get("url"),
                submolt.get("id") if submolt else None,
                submolt.get("name") if submolt else None,
                submolt.get("display_name") if submolt else None,
                author.get("id") if author else None,
                author.get("name") if author else None,
                upvotes,
                downvotes,
                cc,
                post.get("created_at"),
            ))

            if author and author.get("id"):
                agent_rows.append((author["id"], author.get("name")))

            if submolt and submolt.get("id"):
                submolt_rows.append((
                    submolt["id"],
                    submolt.get("name"),
                    submolt.get("display_name"),
                ))

            if pid in stored_counts:
                total_updated += 1
                if cc > stored_counts[pid]:
                    changed_ids.append(pid)
            else:
                total_new += 1
                if cc > 0:
                    changed_ids.append(pid)

    session = await _new_session()
    try:
        while not done:
            if shutdown and shutdown.is_set():
                logger.info("Phase 1 interrupted by shutdown signal")
                break

            data = await fetch_posts_page(session, offset, page_size, "new")

            # Rate limited — cool down
            if data is RATE_LIMITED:
                logger.info("  Rate limited at offset %d — cooling down 60s …", offset)
                await asyncio.sleep(60)
                continue

            if data is None:
                logger.warning("Skipping failed page at offset %d", offset)
                offset += page_size
                continue

            posts = data.get("posts", [])
            if not posts:
                break

            _process_posts(posts)

            if not data.get("has_more", False):
                break

            offset += page_size

            # Flush to DB every ~5000 posts
            if len(post_rows) >= 5000:
                _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)
                logger.info("  Scanned %d posts, %d need detail fetch …",
                            total_seen, len(changed_ids))
                snapshots, post_rows, agent_rows, submolt_rows = [], [], [], []

            await asyncio.sleep(0.7)
    finally:
        await session.close()

    # Final flush
    if post_rows:
        _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)

    logger.info("Phase 1 complete: %d seen, %d new, %d updated, %d need detail fetch",
                total_seen, total_new, total_updated, len(changed_ids))
    return changed_ids


def _flush_phase1(db_path: str, snapshots, post_rows, agent_rows, submolt_rows):
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    cur.executemany('''
        INSERT OR IGNORE INTO post_snapshots
        (post_id, upvotes, downvotes, comment_count, recorded_at)
        VALUES (?, ?, ?, ?, ?)
    ''', snapshots)
    cur.executemany('''
        INSERT OR REPLACE INTO posts
        (id, title, content, url, submolt_id, submolt, submolt_display,
         author_id, author_name, upvotes, downvotes, comment_count, created_at, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', post_rows)
    cur.executemany('''
        INSERT OR IGNORE INTO agents (id, name) VALUES (?, ?)
    ''', agent_rows)
    cur.executemany('''
        INSERT OR REPLACE INTO submolts (id, name, display_name) VALUES (?, ?, ?)
    ''', submolt_rows)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Phase 2 — Targeted detail fetch
# ---------------------------------------------------------------------------
async def phase2_detail_fetch(
    db_path: str,
    crawl_ts: str,
    post_ids: List[str],
    concurrency: int = 20,
    shutdown: asyncio.Event = None,
):
    """Fetch /posts/{id} for each post_id; merge comments, save agent profiles."""
    if not post_ids:
        logger.info("=== Phase 2: nothing to fetch ===")
        return
    logger.info("=== Phase 2: detail fetch for %d posts ===", len(post_ids))

    session = await _new_session()
    sem = asyncio.Semaphore(concurrency)
    chunk_size = 100
    total_comments_new = 0
    total_profiles = 0

    for i in range(0, len(post_ids), chunk_size):
        if shutdown and shutdown.is_set():
            logger.info("Phase 2 interrupted by shutdown signal")
            break

        chunk = post_ids[i : i + chunk_size]
        tasks = [fetch_post_details(session, pid, sem) for pid in chunk]
        results = await asyncio.gather(*tasks)

        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()

        for pid, result in zip(chunk, results):
            if result is None or not result.get("success"):
                continue

            post_data = result.get("post", {})
            comments = result.get("comments", [])

            # Save full author + agent_snapshot
            author = post_data.get("author", {}) or {}
            if author and author.get("id"):
                save_agent_full(cur, author)
                cur.execute('''
                    INSERT OR IGNORE INTO agent_snapshots
                    (agent_id, recorded_at, karma, follower_count, following_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    author["id"], crawl_ts,
                    author.get("karma", 0),
                    author.get("follower_count", 0),
                    author.get("following_count", 0),
                ))
                total_profiles += 1

            # Merge comments (INSERT OR IGNORE)
            flat = flatten_comments(comments, pid)
            for c in flat:
                ca = c.get("author", {}) or {}
                if ca and ca.get("id"):
                    cur.execute('''
                        INSERT OR IGNORE INTO agents (id, name) VALUES (?, ?)
                    ''', (ca["id"], ca.get("name")))
                cur.execute('''
                    INSERT OR IGNORE INTO comments
                    (id, post_id, parent_id, content, author_id, author_name,
                     upvotes, downvotes, depth, created_at, crawled_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    c.get("id"), c["_post_id"], c["_parent_id"],
                    c.get("content"),
                    ca.get("id") if ca else None,
                    ca.get("name") if ca else None,
                    c.get("upvotes", 0), c.get("downvotes", 0),
                    c["_depth"], c.get("created_at"),
                ))
                if cur.rowcount > 0:
                    total_comments_new += 1

        conn.commit()
        conn.close()

        progress = min(i + chunk_size, len(post_ids))
        logger.info("  Phase 2 progress: %d/%d posts, %d new comments, %d profiles",
                     progress, len(post_ids), total_comments_new, total_profiles)

    await session.close()
    logger.info("Phase 2 complete: %d new comments, %d profiles saved",
                total_comments_new, total_profiles)


# ---------------------------------------------------------------------------
# Phase 3a — Agent sweep
# ---------------------------------------------------------------------------
async def phase3a_agent_sweep(
    db_path: str,
    crawl_ts: str,
    concurrency: int = 20,
    max_agents: int = 5000,
    max_age_hours: float = 12.0,
    shutdown: asyncio.Event = None,
):
    """Refresh agent profiles not updated in the last max_age_hours."""
    logger.info("=== Phase 3a: agent sweep (stale > %.1fh, up to %d) ===",
                max_age_hours, max_agents)

    conn = sqlite3.connect(db_path)
    rows = conn.execute('''
        SELECT a.id, p.id AS post_id
        FROM agents a
        JOIN posts p ON p.author_id = a.id
        WHERE a.last_updated < datetime('now', ?)
           OR a.last_updated IS NULL
        GROUP BY a.id
        LIMIT ?
    ''', (f'-{max_age_hours} hours', max_agents)).fetchall()
    conn.close()

    if not rows:
        logger.info("Phase 3a: no stale agents found")
        return

    logger.info("Phase 3a: %d stale agents to refresh", len(rows))

    session = await _new_session()
    sem = asyncio.Semaphore(concurrency)
    chunk_size = 100
    refreshed = 0

    for i in range(0, len(rows), chunk_size):
        if shutdown and shutdown.is_set():
            logger.info("Phase 3a interrupted by shutdown signal")
            break

        chunk = rows[i : i + chunk_size]
        tasks = [fetch_post_details(session, r[1], sem) for r in chunk]
        results = await asyncio.gather(*tasks)

        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()

        for (agent_id, _post_id), result in zip(chunk, results):
            if result is None or not result.get("success"):
                continue
            author = result.get("post", {}).get("author", {}) or {}
            if author and author.get("id"):
                save_agent_full(cur, author)
                cur.execute('''
                    INSERT OR IGNORE INTO agent_snapshots
                    (agent_id, recorded_at, karma, follower_count, following_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    author["id"], crawl_ts,
                    author.get("karma", 0),
                    author.get("follower_count", 0),
                    author.get("following_count", 0),
                ))
                refreshed += 1

        conn.commit()
        conn.close()

        progress = min(i + chunk_size, len(rows))
        if progress % 500 == 0 or progress == len(rows):
            logger.info("  Phase 3a progress: %d/%d agents refreshed", progress, len(rows))

    await session.close()
    logger.info("Phase 3a complete: %d agents refreshed", refreshed)


# ---------------------------------------------------------------------------
# Phase 3b — Submolt sweep
# ---------------------------------------------------------------------------
async def phase3b_submolt_sweep(
    db_path: str,
    crawl_ts: str,
    shutdown: asyncio.Event = None,
):
    """Scan all /submolts pages, update submolts table and record snapshots."""
    logger.info("=== Phase 3b: submolt sweep ===")
    session = await _new_session()
    offset = 0
    limit = 100
    total = 0

    while True:
        if shutdown and shutdown.is_set():
            logger.info("Phase 3b interrupted by shutdown signal")
            break

        data = await fetch_submolts_page(session, offset, limit)
        submolts = data.get("submolts", [])
        if not submolts:
            break

        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        for s in submolts:
            cur.execute('''
                INSERT OR REPLACE INTO submolts
                (id, name, display_name, description, subscriber_count,
                 created_at, last_activity_at, featured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                s.get("id"), s.get("name"), s.get("display_name"),
                s.get("description"), s.get("subscriber_count", 0),
                s.get("created_at"), s.get("last_activity_at"),
                s.get("featured_at"),
            ))
            cur.execute('''
                INSERT OR IGNORE INTO submolt_snapshots
                (submolt_id, recorded_at, subscriber_count)
                VALUES (?, ?, ?)
            ''', (s.get("id"), crawl_ts, s.get("subscriber_count", 0)))
        conn.commit()
        conn.close()

        total += len(submolts)
        logger.info("  Submolts fetched: %d (API total: %s)",
                     total, data.get("count", "?"))

        offset += limit
        api_count = data.get("count", 0)
        if api_count and offset >= api_count:
            break
        await asyncio.sleep(0.1)

    await session.close()
    logger.info("Phase 3b complete: %d submolts saved", total)


# ---------------------------------------------------------------------------
# Background task: Homepage stats (every 30 minutes)
# ---------------------------------------------------------------------------
async def homepage_stats_loop(db_path: str, interval: int, shutdown: asyncio.Event):
    """Background task to fetch homepage stats every `interval` seconds."""
    logger.info("Homepage stats collector started (interval=%ds)", interval)

    while not shutdown.is_set():
        try:
            async with aiohttp.ClientSession(headers=HTTP_HEADERS) as session:
                stats = await fetch_homepage_stats(session)
                if stats:
                    recorded_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    conn = sqlite3.connect(db_path, timeout=30)
                    conn.execute('''
                        INSERT OR REPLACE INTO homepage_stats
                        (recorded_at, agents, submolts, posts, comments)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        recorded_at,
                        stats.get("agents", 0),
                        stats.get("submolts", 0),
                        stats.get("posts", 0),
                        stats.get("comments", 0),
                    ))
                    conn.commit()
                    conn.close()
                    logger.info("Homepage stats saved: agents=%s, posts=%s, comments=%s, submolts=%s",
                                f"{stats.get('agents', 0):,}",
                                f"{stats.get('posts', 0):,}",
                                f"{stats.get('comments', 0):,}",
                                f"{stats.get('submolts', 0):,}")
                else:
                    logger.warning("Failed to fetch homepage stats")
        except Exception as exc:
            logger.exception("Error in homepage stats collector: %s", exc)

        # Wait for interval or shutdown
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break  # shutdown was set
        except asyncio.TimeoutError:
            pass  # normal: interval elapsed

    logger.info("Homepage stats collector stopped")


# ---------------------------------------------------------------------------
# Main run / daemon loop
# ---------------------------------------------------------------------------
async def run_cycle(
    db_path: str,
    phases: Set[int],
    concurrency: int,
    detail_concurrency: int,
    agent_batch: int,
    agent_max_age: float,
    shutdown: asyncio.Event,
):
    """Execute one full maintenance cycle."""
    crawl_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Cycle start — crawl_timestamp=%s  phases=%s", crawl_ts, sorted(phases))

    changed_ids: List[str] = []

    # Phase 1
    if 1 in phases:
        changed_ids = await phase1_listing_scan(
            db_path, crawl_ts,
            concurrency=concurrency,
            shutdown=shutdown,
        )

    # Phase 2
    if 2 in phases:
        await phase2_detail_fetch(
            db_path, crawl_ts,
            post_ids=changed_ids,
            concurrency=detail_concurrency,
            shutdown=shutdown,
        )

    # Phase 3
    if 3 in phases:
        await phase3a_agent_sweep(
            db_path, crawl_ts,
            concurrency=detail_concurrency,
            max_agents=agent_batch,
            max_age_hours=agent_max_age,
            shutdown=shutdown,
        )
        await phase3b_submolt_sweep(
            db_path, crawl_ts,
            shutdown=shutdown,
        )

    logger.info("Cycle complete.")


async def main_async(args):
    shutdown = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    phases = set(args.phase) if args.phase else {1, 2, 3}

    init_db(args.db)

    # Start background homepage stats collector (runs independently every 30 min)
    homepage_task = None
    if args.loop or args.homepage_stats:
        homepage_task = asyncio.create_task(
            homepage_stats_loop(args.db, args.homepage_interval, shutdown)
        )

    if args.loop:
        cycle = 0
        while not shutdown.is_set():
            cycle += 1
            logger.info("===== Daemon cycle %d =====", cycle)
            t0 = time.time()
            try:
                await run_cycle(
                    db_path=args.db,
                    phases=phases,
                    concurrency=args.concurrency,
                    detail_concurrency=args.detail_concurrency,
                    agent_batch=args.agent_batch,
                    agent_max_age=args.agent_max_age,
                    shutdown=shutdown,
                )
            except Exception:
                logger.exception("Unhandled exception in cycle %d — will retry next cycle", cycle)
            elapsed = time.time() - t0
            logger.info("Cycle %d finished in %.1fs (%.1f min)", cycle, elapsed, elapsed / 60)

            # Sleep with shutdown check
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=args.interval)
            except asyncio.TimeoutError:
                pass  # normal: interval elapsed

        logger.info("Shutdown signal received, exiting.")

        # Wait for homepage task to finish
        if homepage_task:
            await homepage_task
    else:
        try:
            await run_cycle(
                db_path=args.db,
                phases=phases,
                concurrency=args.concurrency,
                detail_concurrency=args.detail_concurrency,
                agent_batch=args.agent_batch,
                agent_max_age=args.agent_max_age,
                shutdown=shutdown,
            )
        except Exception:
            logger.exception("Unhandled exception")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Moltbook continuous DB maintenance daemon",
    )
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="Path to SQLite database (default: %(default)s)")
    parser.add_argument("--phase", type=int, action="append", choices=[1, 2, 3],
                        help="Run specific phase(s). Can be repeated. Default: all three.")
    parser.add_argument("--loop", action="store_true",
                        help="Run continuously in daemon mode")
    parser.add_argument("--interval", type=int, default=3600,
                        help="Seconds between cycles in daemon mode (default: 3600)")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="Phase 1 listing concurrency (default: 5)")
    parser.add_argument("--detail-concurrency", type=int, default=20,
                        help="Phase 2/3 detail fetch concurrency (default: 20)")
    parser.add_argument("--agent-batch", type=int, default=5000,
                        help="Max agents per sweep in Phase 3a (default: 5000)")
    parser.add_argument("--agent-max-age", type=float, default=12.0,
                        help="Staleness threshold in hours for Phase 3a (default: 12.0)")
    parser.add_argument("--homepage-stats", action="store_true",
                        help="Enable homepage stats collection (auto-enabled in --loop mode)")
    parser.add_argument("--homepage-interval", type=int, default=1800,
                        help="Seconds between homepage stats fetches (default: 1800 = 30 min)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Moltbook DB Maintenance")
    logger.info("  db=%s  phases=%s  loop=%s  interval=%ds",
                args.db, args.phase or [1, 2, 3], args.loop, args.interval)
    logger.info("  concurrency=%d  detail_concurrency=%d",
                args.concurrency, args.detail_concurrency)
    logger.info("  agent_batch=%d  agent_max_age=%.1fh",
                args.agent_batch, args.agent_max_age)
    logger.info("  homepage_stats=%s  homepage_interval=%ds",
                args.homepage_stats or args.loop, args.homepage_interval)
    logger.info("=" * 60)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
