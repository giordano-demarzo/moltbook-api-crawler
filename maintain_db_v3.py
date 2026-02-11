#!/usr/bin/env python3
"""
Continuous DB maintenance daemon for Moltbook — v3 (3-worker concurrent crawler).

Three concurrent workers + background stats:
  Worker 1 (Discovery Scanner) — every ~2.5 min, scans 11 sort+time combos
  Worker 2 (Comment Fetcher)   — continuous, pulls from priority queue
  Worker 3 (Background Scanner)— ~60 min cycles, full listing + random sample
  Homepage stats               — every 30 min

Usage:
  python maintain_db_v3.py --proxies proxies.txt        # typical usage
  python maintain_db_v3.py --listing-depth 500000        # background listing depth (default: 1000000)
  python maintain_db_v3.py --proxy-split 20 60 20       # proxy % split (default: 20 60 20)
  python maintain_db_v3.py --discovery-interval 150     # seconds between scans (default: 150)
  python maintain_db_v3.py --discovery-depth 5000       # posts per sort+time (default: 5000)
  python maintain_db_v3.py -v                           # verbose logging
"""
import argparse
import asyncio
import heapq
import itertools
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
from aiohttp_socks import ProxyConnector


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
API_BASE = "https://www.moltbook.com/api/v1"
DEFAULT_DB = "data/moltbook.db"
HTTP_HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}
COMMENT_SORTS = ("old", "new", "top", "controversial")

logger = logging.getLogger("maintain_db")

# ---------------------------------------------------------------------------
# Proxy rotation
# ---------------------------------------------------------------------------
_PROXIES: List[str] = []
_proxy_counter = itertools.count()


def _next_proxy() -> Optional[str]:
    """Return the next proxy URL in round-robin order, or None if no proxies."""
    if not _PROXIES:
        return None
    return _PROXIES[next(_proxy_counter) % len(_PROXIES)]


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
            x_following_count INTEGER DEFAULT 0,
            x_verified INTEGER DEFAULT 0,
            x_avatar TEXT,
            avatar_url TEXT,
            last_active TIMESTAMP,
            is_active INTEGER DEFAULT 0,
            is_claimed INTEGER DEFAULT 0,
            created_at TIMESTAMP,
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

    # Schema migration: add new agent columns (idempotent)
    for col, col_def in [
        ("avatar_url", "TEXT"),
        ("last_active", "TIMESTAMP"),
        ("is_active", "INTEGER DEFAULT 0"),
        ("is_claimed", "INTEGER DEFAULT 0"),
        ("x_following_count", "INTEGER DEFAULT 0"),
        ("x_avatar", "TEXT"),
        ("created_at", "TIMESTAMP"),
    ]:
        try:
            cur.execute(f"ALTER TABLE agents ADD COLUMN {col} {col_def}")
        except sqlite3.OperationalError:
            pass  # column already exists

    # Schema migration: add unreachable comment tracking (idempotent)
    try:
        cur.execute("ALTER TABLE posts ADD COLUMN comments_unreachable_at INTEGER DEFAULT NULL")
    except sqlite3.OperationalError:
        pass

    # Schema migration: v3 velocity tracking columns (idempotent)
    for col, col_def in [
        ("last_comment_check_at", "TIMESTAMP DEFAULT NULL"),
        ("last_known_cc", "INTEGER DEFAULT NULL"),
        ("comment_velocity", "REAL DEFAULT NULL"),
        ("deleted_at", "TIMESTAMP DEFAULT NULL"),
    ]:
        try:
            cur.execute(f"ALTER TABLE posts ADD COLUMN {col} {col_def}")
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()
    logger.info("Database initialized: %s", db_path)


def get_stored_comment_counts(db_path: str) -> Dict[str, int]:
    """Return {post_id: actual_saved_comments} for every post in DB."""
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
# Comment / agent helpers
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
         x_handle, x_name, x_bio, x_follower_count, x_following_count,
         x_verified, x_avatar, avatar_url, last_active, is_active,
         is_claimed, created_at, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
        owner.get("x_following_count", 0),
        1 if owner.get("x_verified") else 0,
        owner.get("x_avatar"),
        agent.get("avatar_url"),
        agent.get("last_active"),
        1 if agent.get("is_active") else 0,
        1 if agent.get("is_claimed") else 0,
        agent.get("created_at"),
    ))


def _save_flat_comment(cursor: sqlite3.Cursor, c: Dict, post_id: str):
    """INSERT OR IGNORE a single flat comment (from sort endpoints) and its author."""
    ca = c.get("author", {}) or {}
    if ca and ca.get("id"):
        cursor.execute(
            'INSERT OR IGNORE INTO agents (id, name) VALUES (?, ?)',
            (ca["id"], ca.get("name")),
        )
    cursor.execute('''
        INSERT OR IGNORE INTO comments
        (id, post_id, parent_id, content, author_id, author_name,
         upvotes, downvotes, depth, created_at, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        c.get("id"), post_id, c.get("parent_id"),
        c.get("content"),
        ca.get("id") if ca else None,
        ca.get("name") if ca else None,
        c.get("upvotes", 0), c.get("downvotes", 0),
        0,  # flat endpoint doesn't provide nesting depth
        c.get("created_at"),
    ))
    return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# HTTP fetchers (with retry / back-off)
# ---------------------------------------------------------------------------
RATE_LIMITED = {"_rate_limited": True}


async def fetch_posts_page(session: aiohttp.ClientSession, offset: int,
                           limit: int = 50, sort: str = "new",
                           time: str = None,
                           retries: int = 10) -> Optional[Dict]:
    limit = min(limit, 50)
    url = f"{API_BASE}/posts?limit={limit}&offset={offset}&sort={sort}"
    if time:
        url += f"&time={time}"
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
                    if resp.status == 404:
                        return {"_deleted": True, "post_id": post_id}
                    if resp.status == 429:
                        cooldown = 5.0 if _PROXIES else 60.0
                        delay = (cooldown / 12) * attempt + random.uniform(0, 2)
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


async def fetch_post_comments(session: aiohttp.ClientSession, post_id: str,
                              sort: str, semaphore: asyncio.Semaphore,
                              retries: int = 5) -> Optional[Dict]:
    """Fetch /posts/{post_id}/comments?sort={sort}&limit=100."""
    async with semaphore:
        url = f"{API_BASE}/posts/{post_id}/comments?sort={sort}&limit=100"
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 404:
                        return {"_deleted": True}
                    if resp.status == 429:
                        cooldown = 5.0 if _PROXIES else 60.0
                        delay = (cooldown / 12) * attempt + random.uniform(0, 2)
                        if attempt < retries:
                            await asyncio.sleep(delay)
                        continue
                    if resp.status >= 500:
                        logger.debug("HTTP %d for post %s comments sort=%s (attempt %d/%d)",
                                     resp.status, post_id, sort, attempt, retries)
                    else:
                        logger.warning("HTTP %d for post %s comments sort=%s (attempt %d/%d)",
                                       resp.status, post_id, sort, attempt, retries)
            except Exception as exc:
                logger.warning("Exception fetching comments post %s sort=%s: %s (attempt %d/%d)",
                               post_id, sort, exc, attempt, retries)
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


async def fetch_agent_profile(session: aiohttp.ClientSession, agent_name: str,
                              semaphore: asyncio.Semaphore,
                              retries: int = 5) -> Optional[Dict]:
    """Fetch /agents/profile?name={agent_name}."""
    async with semaphore:
        url = f"{API_BASE}/agents/profile?name={agent_name}"
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 404:
                        return {"_deleted": True, "agent_name": agent_name}
                    if resp.status == 429:
                        cooldown = 5.0 if _PROXIES else 60.0
                        delay = (cooldown / 12) * attempt + random.uniform(0, 2)
                        if attempt < retries:
                            await asyncio.sleep(delay)
                        continue
                    if resp.status >= 500:
                        logger.debug("HTTP %d for agent profile %s (attempt %d/%d)",
                                     resp.status, agent_name, attempt, retries)
                    else:
                        logger.warning("HTTP %d for agent profile %s (attempt %d/%d)",
                                       resp.status, agent_name, attempt, retries)
            except Exception as exc:
                logger.warning("Exception fetching agent profile %s: %s (attempt %d/%d)",
                               agent_name, exc, attempt, retries)
            if attempt < retries:
                await asyncio.sleep(1.0 * attempt)
    return None


# ---------------------------------------------------------------------------
# Session pool
# ---------------------------------------------------------------------------
async def _new_session(proxy: Optional[str] = None) -> aiohttp.ClientSession:
    """Create a fresh HTTP session, optionally routed through a proxy."""
    if proxy:
        connector = ProxyConnector.from_url(proxy)
        return aiohttp.ClientSession(connector=connector, headers=HTTP_HEADERS)
    return aiohttp.ClientSession(headers=HTTP_HEADERS)


def _next_session(sessions: List[aiohttp.ClientSession]) -> aiohttp.ClientSession:
    """Pick the next session from a pool in round-robin order."""
    idx = next(_proxy_counter) % len(sessions)
    return sessions[idx]


async def _create_session_pool(proxy_list: List[str] = None) -> List[aiohttp.ClientSession]:
    """Create one session per proxy, or a single direct session if no proxies.

    If proxy_list is given, use that subset instead of the global _PROXIES.
    """
    proxies = proxy_list if proxy_list is not None else _PROXIES
    if not proxies:
        return [await _new_session()]
    return [await _new_session(proxy=p) for p in proxies]


async def _close_session_pool(sessions: List[aiohttp.ClientSession]):
    for s in sessions:
        await s.close()


# ---------------------------------------------------------------------------
# DB flush helper
# ---------------------------------------------------------------------------
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
# Homepage stats background task
# ---------------------------------------------------------------------------
async def homepage_stats_loop(db_path: str, interval: int, shutdown: asyncio.Event):
    """Background task to fetch homepage stats every `interval` seconds."""
    logger.info("Homepage stats collector started (interval=%ds)", interval)

    while not shutdown.is_set():
        try:
            session = await _new_session(_next_proxy())
            try:
                stats = await fetch_homepage_stats(session)
            finally:
                await session.close()
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

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Homepage stats collector stopped")


# ---------------------------------------------------------------------------
# Priority Queue
# ---------------------------------------------------------------------------
class CommentQueue:
    """Thread-safe async priority queue for comment fetching.

    Items are (priority, post_id, api_cc). Lower priority value = higher urgency.
    """

    def __init__(self):
        self._heap: List[Tuple[float, str, int]] = []
        self._in_queue: Set[str] = set()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self.total_pushed = 0
        self.total_popped = 0

    async def push(self, post_id: str, api_cc: int, priority: float):
        """Add a post to the queue if not already present."""
        async with self._lock:
            if post_id in self._in_queue:
                return
            self._in_queue.add(post_id)
            heapq.heappush(self._heap, (priority, post_id, api_cc))
            self.total_pushed += 1
            self._not_empty.set()

    async def push_batch(self, items: List[Tuple[str, int, float]]):
        """Add multiple (post_id, api_cc, priority) items."""
        async with self._lock:
            added = 0
            for post_id, api_cc, priority in items:
                if post_id in self._in_queue:
                    continue
                self._in_queue.add(post_id)
                heapq.heappush(self._heap, (priority, post_id, api_cc))
                added += 1
            self.total_pushed += added
            if self._heap:
                self._not_empty.set()
            return added

    async def pop(self) -> Tuple[str, int]:
        """Pop highest-priority item. Blocks if empty."""
        while True:
            await self._not_empty.wait()
            async with self._lock:
                if self._heap:
                    _priority, post_id, api_cc = heapq.heappop(self._heap)
                    self._in_queue.discard(post_id)
                    self.total_popped += 1
                    if not self._heap:
                        self._not_empty.clear()
                    return post_id, api_cc
                self._not_empty.clear()

    async def pop_batch(self, max_items: int) -> List[Tuple[str, int]]:
        """Pop up to max_items. Blocks until at least one is available."""
        while True:
            await self._not_empty.wait()
            async with self._lock:
                if not self._heap:
                    self._not_empty.clear()
                    continue
                result = []
                while self._heap and len(result) < max_items:
                    _priority, post_id, api_cc = heapq.heappop(self._heap)
                    self._in_queue.discard(post_id)
                    result.append((post_id, api_cc))
                self.total_popped += len(result)
                if not self._heap:
                    self._not_empty.clear()
                return result

    def qsize(self) -> int:
        """Current queue size (approximate, no lock)."""
        return len(self._heap)

    def in_queue(self, post_id: str) -> bool:
        """Check if post_id is currently in the queue (approximate, no lock)."""
        return post_id in self._in_queue


# ---------------------------------------------------------------------------
# Queue stats reporter
# ---------------------------------------------------------------------------
async def queue_stats_loop(queue: CommentQueue, shutdown: asyncio.Event,
                           interval: int = 30):
    """Log queue throughput and size every `interval` seconds."""
    logger.info("Queue stats reporter started (interval=%ds)", interval)
    last_popped = 0
    last_pushed = 0
    t_start = time.time()

    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

        now = time.time()
        elapsed = now - t_start
        cur_popped = queue.total_popped
        cur_pushed = queue.total_pushed
        popped_delta = cur_popped - last_popped
        pushed_delta = cur_pushed - last_pushed
        rate = popped_delta / interval if interval else 0
        last_popped = cur_popped
        last_pushed = cur_pushed

        logger.info("QUEUE STATS | pending=%d | +%d pushed | -%d processed (%.1f/s) | "
                     "total: %d pushed, %d processed | uptime %.0fs",
                     queue.qsize(), pushed_delta, popped_delta, rate,
                     cur_pushed, cur_popped, elapsed)

    logger.info("Queue stats reporter stopped")


# ---------------------------------------------------------------------------
# Post data processing helper (shared by discovery + background scanners)
# ---------------------------------------------------------------------------
def _process_post_data(post: Dict, crawl_ts: str,
                       snapshots: list, post_rows: list,
                       agent_rows: list, submolt_rows: list):
    """Extract post data into accumulation buffers. Returns (post_id, comment_count) or None."""
    pid = post.get("id")
    if not pid:
        return None

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

    return pid, cc


def _compute_priority(deficit: int, api_cc: int, created_at: str = None) -> float:
    """Compute queue priority (lower = more urgent).

    Factors:
    - Deficit size: larger deficit → lower priority value
    - Post age: newer posts → lower priority value
    """
    # Base: inverse of deficit (capped to avoid division by zero)
    priority = 1.0 / max(deficit, 1)

    # Boost for larger total comment counts (more activity = more urgency)
    if api_cc > 100:
        priority *= 0.5
    elif api_cc > 50:
        priority *= 0.7

    # Boost for recency (if created_at is available)
    if created_at:
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            age_hours = (datetime.utcnow().replace(tzinfo=created.tzinfo) - created).total_seconds() / 3600
            if age_hours < 1:
                priority *= 0.3
            elif age_hours < 6:
                priority *= 0.5
            elif age_hours < 24:
                priority *= 0.7
        except (ValueError, TypeError):
            pass

    return priority


# ---------------------------------------------------------------------------
# Worker 1 — Discovery Scanner
# ---------------------------------------------------------------------------
DISCOVERY_COMBOS = [
    ("top", "hour"), ("top", "day"), ("top", "week"), ("top", "month"), ("top", "all"),
    ("comments", "hour"), ("comments", "day"), ("comments", "week"),
    ("comments", "month"), ("comments", "all"),
    ("new", None),
]


async def discovery_scanner(db_path: str, queue: CommentQueue,
                            sessions: List[aiohttp.ClientSession],
                            shutdown: asyncio.Event,
                            interval: int = 150,
                            depth: int = 10000):
    """Worker 1: Scans 11 sort+time combos every ~interval seconds, pushes deficit posts to queue."""
    logger.info("Discovery scanner started (interval=%ds, depth=%d, combos=%d, sessions=%d)",
                interval, depth, len(DISCOVERY_COMBOS), len(sessions))

    cycle = 0
    while not shutdown.is_set():
        cycle += 1
        t0 = time.time()
        try:
            await _discovery_cycle(db_path, queue, sessions, shutdown, depth, cycle)
        except Exception as exc:
            logger.exception("Discovery scanner cycle %d error: %s", cycle, exc)

        elapsed = time.time() - t0
        logger.info("Discovery scanner cycle %d complete in %.1fs, queue=%d",
                     cycle, elapsed, queue.qsize())

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Discovery scanner stopped")


async def _discovery_cycle(db_path: str, queue: CommentQueue,
                           sessions: List[aiohttp.ClientSession],
                           shutdown: asyncio.Event,
                           depth: int, cycle: int):
    """One discovery scan cycle: all 11 combos in parallel."""
    crawl_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    stored_counts = get_stored_comment_counts(db_path)
    page_size = 50
    max_pages = depth // page_size  # e.g. 5000/50 = 100 pages per combo

    # Accumulation buffers
    snapshots: List[Tuple] = []
    post_rows: List[Tuple] = []
    agent_rows: List[Tuple] = []
    submolt_rows: List[Tuple] = []
    data_lock = asyncio.Lock()
    total_seen = 0
    total_deficit = 0
    total_pushed = 0

    sem = asyncio.Semaphore(max(len(sessions), 10))

    async def _fetch_one_page(sort: str, time_filter: Optional[str], offset: int):
        """Fetch a single page under the semaphore."""
        async with sem:
            session = _next_session(sessions)
            return await fetch_posts_page(session, offset, page_size, sort, time=time_filter, retries=5)

    # Track per-combo progress
    combo_stats: Dict[str, Dict] = {}
    combo_stats_lock = asyncio.Lock()

    async def _scan_combo(sort: str, time_filter: Optional[str]):
        """Paginate one sort+time combo up to max_pages."""
        nonlocal total_seen, total_deficit, total_pushed
        combo_key = f"{sort}/{time_filter or '-'}"
        combo_seen = 0
        combo_deficit = 0
        combo_queued = 0
        combo_pages = 0

        for page_idx in range(max_pages):
            if shutdown.is_set():
                break

            offset = page_idx * page_size
            data = await _fetch_one_page(sort, time_filter, offset)

            if data is RATE_LIMITED or data is None:
                logger.debug("Discovery: skip sort=%s time=%s offset=%d (rate limited/failed)",
                             sort, time_filter, offset)
                break

            posts = data.get("posts", [])
            if not posts:
                break

            combo_pages += 1

            # Collect deficit items from this page
            page_deficit: List[Tuple[str, int, float]] = []

            async with data_lock:
                for post in posts:
                    result = _process_post_data(post, crawl_ts, snapshots, post_rows,
                                                agent_rows, submolt_rows)
                    if result is None:
                        continue
                    pid, cc = result
                    total_seen += 1
                    combo_seen += 1

                    stored = stored_counts.get(pid, 0)
                    deficit = cc - stored
                    if deficit > 0:
                        total_deficit += 1
                        combo_deficit += 1
                        if not queue.in_queue(pid):
                            priority = _compute_priority(deficit, cc, post.get("created_at"))
                            page_deficit.append((pid, cc, priority))

                # Flush DB periodically
                if len(post_rows) >= 5000:
                    _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)
                    snapshots.clear()
                    post_rows.clear()
                    agent_rows.clear()
                    submolt_rows.clear()

            # Push deficits to queue immediately (outside data_lock)
            if page_deficit:
                added = await queue.push_batch(page_deficit)
                total_pushed += added
                combo_queued += added

            if not data.get("has_more", False):
                break

        async with combo_stats_lock:
            combo_stats[combo_key] = {
                "pages": combo_pages, "posts": combo_seen,
                "deficit": combo_deficit, "queued": combo_queued,
            }

    # Launch all 11 combos concurrently
    combo_tasks = [asyncio.create_task(_scan_combo(sort, tf))
                   for sort, tf in DISCOVERY_COMBOS]
    await asyncio.gather(*combo_tasks)

    # Final flush
    if post_rows:
        _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)

    # Log per-combo breakdown
    for combo_key, stats in sorted(combo_stats.items()):
        logger.info("  Discovery %-20s  %3d pages  %5d posts  %5d deficit  %4d queued",
                     combo_key, stats["pages"], stats["posts"],
                     stats["deficit"], stats["queued"])

    logger.info("Discovery cycle %d: scanned %d posts, %d deficit, %d pushed to queue",
                 cycle, total_seen, total_deficit, total_pushed)


# ---------------------------------------------------------------------------
# Worker 2 — Comment Fetcher
# ---------------------------------------------------------------------------
async def _refresh_stale_agents(db_path: str, sessions: List[aiohttp.ClientSession],
                                sem: asyncio.Semaphore, shutdown: asyncio.Event,
                                batch_size: int = 1000) -> Tuple[int, int, int]:
    """Fetch profiles for agents not updated in 24h. Returns (refreshed, bonus_comments, bonus_posts)."""
    conn = sqlite3.connect(db_path, timeout=30)
    rows = conn.execute('''
        SELECT id, name FROM agents
        WHERE (last_updated < datetime('now', '-24 hours') OR last_updated IS NULL)
          AND name IS NOT NULL
        ORDER BY last_updated ASC NULLS FIRST
        LIMIT ?
    ''', (batch_size,)).fetchall()
    conn.close()

    if not rows:
        return 0, 0, 0

    logger.info("Agent refresh: %d stale agents to update", len(rows))

    refreshed = 0
    bonus_comments = 0
    bonus_posts = 0
    chunk_size = 100
    crawl_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for i in range(0, len(rows), chunk_size):
        if shutdown.is_set():
            break

        chunk = rows[i : i + chunk_size]
        tasks = [fetch_agent_profile(_next_session(sessions), name, sem)
                 for (_agent_id, name) in chunk]
        results = await asyncio.gather(*tasks)

        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()

        for (agent_id, agent_name), result in zip(chunk, results):
            if result is None:
                continue

            # Mark deleted agents so we skip them next time
            if result.get("_deleted"):
                cur.execute(
                    "UPDATE agents SET last_updated = CURRENT_TIMESTAMP, description = '[deleted]' WHERE id = ?",
                    (agent_id,),
                )
                continue

            # Profile endpoint wraps in {"success": true, "agent": {...}}
            if isinstance(result, dict) and "agent" in result:
                agent_data = result["agent"]
            elif isinstance(result, dict) and result.get("id"):
                agent_data = result
            else:
                continue

            if agent_data and agent_data.get("id"):
                save_agent_full(cur, agent_data)
                # Agent snapshot
                cur.execute('''
                    INSERT OR IGNORE INTO agent_snapshots
                    (agent_id, recorded_at, karma, follower_count, following_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    agent_data["id"], crawl_ts,
                    agent_data.get("karma", 0),
                    agent_data.get("follower_count", 0),
                    agent_data.get("following_count", 0),
                ))
                refreshed += 1

            # Harvest bonus recentComments
            recent_comments = result.get("recentComments", [])
            for c in recent_comments:
                if not c or not c.get("id"):
                    continue
                post_id = c.get("post_id") or c.get("postId")
                if not post_id:
                    continue
                if _save_flat_comment(cur, c, post_id):
                    bonus_comments += 1

            # Harvest bonus recentPosts
            recent_posts = result.get("recentPosts", [])
            for post in recent_posts:
                if not post or not post.get("id"):
                    continue
                pid = post["id"]
                submolt = post.get("submolt", {}) or {}
                cur.execute('''
                    INSERT OR IGNORE INTO posts
                    (id, title, content, upvotes, downvotes, comment_count,
                     created_at, submolt, crawled_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    pid, post.get("title"), post.get("content"),
                    post.get("upvotes", 0), post.get("downvotes", 0),
                    post.get("comment_count", 0), post.get("created_at"),
                    submolt.get("name"),
                ))
                if cur.rowcount > 0:
                    bonus_posts += 1

        conn.commit()
        conn.close()

        progress = min(i + chunk_size, len(rows))
        logger.info("Agent refresh: %d/%d done, %d refreshed, %d bonus comments, %d bonus posts",
                     progress, len(rows), refreshed, bonus_comments, bonus_posts)

    logger.info("Agent refresh complete: %d refreshed, %d bonus comments, %d bonus posts",
                refreshed, bonus_comments, bonus_posts)
    return refreshed, bonus_comments, bonus_posts


async def _check_random_posts(db_path: str, queue: CommentQueue,
                              sessions: List[aiohttp.ClientSession],
                              sem: asyncio.Semaphore, shutdown: asyncio.Event,
                              batch_size: int = 1000) -> Tuple[int, int]:
    """Fetch details for random non-deleted posts, push deficits to queue. Returns (checked, pushed)."""
    conn = sqlite3.connect(db_path)
    all_ids = [r[0] for r in conn.execute('SELECT id FROM posts WHERE deleted_at IS NULL').fetchall()]
    conn.close()

    if not all_ids:
        return 0, 0

    sample_ids = random.sample(all_ids, min(batch_size, len(all_ids)))
    logger.info("Random post check: sampling %d posts", len(sample_ids))

    stored_counts = get_stored_comment_counts(db_path)
    chunk_size = max(len(sessions), 5)
    total_checked = 0
    total_pushed = 0
    total_deleted = 0

    for i in range(0, len(sample_ids), chunk_size):
        if shutdown.is_set():
            break

        chunk = sample_ids[i : i + chunk_size]
        tasks = [fetch_post_details(_next_session(sessions), pid, sem) for pid in chunk]
        results = await asyncio.gather(*tasks)

        chunk_deficit: List[Tuple[str, int, float]] = []
        deleted_ids: List[str] = []
        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        for pid, result in zip(chunk, results):
            total_checked += 1
            if result is None:
                continue

            if result.get("_deleted"):
                deleted_ids.append(pid)
                continue

            if not result.get("success"):
                continue

            post_data = result.get("post", {})
            api_cc = post_data.get("comment_count", 0)
            stored = stored_counts.get(pid, 0)
            deficit = api_cc - stored

            author = post_data.get("author")
            if author and author.get("id"):
                save_agent_full(cur, author)

            if deficit > 0 and not queue.in_queue(pid):
                priority = _compute_priority(deficit, api_cc, post_data.get("created_at"))
                chunk_deficit.append((pid, api_cc, priority))

        if deleted_ids:
            cur.executemany(
                'UPDATE posts SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?',
                [(pid,) for pid in deleted_ids],
            )
            total_deleted += len(deleted_ids)
        conn.commit()
        conn.close()

        if chunk_deficit:
            added = await queue.push_batch(chunk_deficit)
            total_pushed += added
            logger.info("  Random posts: pushed %d to queue (%d/%d checked, %d deficit, %d deleted)",
                        added, total_checked, len(sample_ids), total_pushed, total_deleted)
        elif deleted_ids:
            logger.info("  Random posts: %d/%d checked, %d deleted so far",
                        total_checked, len(sample_ids), total_deleted)
        elif total_checked % 200 == 0:
            logger.info("  Random posts: %d/%d checked, %d pushed, %d deleted so far",
                        total_checked, len(sample_ids), total_pushed, total_deleted)

    logger.info("Random post check complete: %d checked, %d pushed to queue, %d deleted",
                total_checked, total_pushed, total_deleted)
    return total_checked, total_pushed


async def comment_fetcher(db_path: str, queue: CommentQueue,
                          sessions: List[aiohttp.ClientSession],
                          shutdown: asyncio.Event):
    """Worker 2: Continuously pulls posts from queue and fetches all 4 comment sorts.
    When queue is empty, enqueues random posts and refreshes stale agents."""
    # High concurrency — fetcher is the bottleneck
    concurrency = max(len(sessions) * 17, 500)
    # Batch size: concurrency / 4 sorts
    batch_size = max(concurrency // 4, 5)
    logger.info("Comment fetcher started (concurrency=%d, batch_size=%d, sessions=%d)",
                concurrency, batch_size, len(sessions))

    sem = asyncio.Semaphore(concurrency)
    total_posts_processed = 0
    total_comments_new = 0

    while not shutdown.is_set():
        # Wait for items with a timeout so we can check shutdown
        try:
            batch = await asyncio.wait_for(
                queue.pop_batch(batch_size),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            # Queue is empty — use idle time to refresh stale agents
            await _refresh_stale_agents(db_path, sessions, sem, shutdown)
            continue

        if not batch:
            continue

        t0 = time.time()

        # Fire 4 comment sort requests per post
        tasks = []
        task_map = []  # (post_id, sort_name)
        for post_id, api_cc in batch:
            for sort in COMMENT_SORTS:
                session = _next_session(sessions)
                tasks.append(fetch_post_comments(session, post_id, sort, sem))
                task_map.append((post_id, sort))

        results = await asyncio.gather(*tasks)

        # Group results by post_id
        post_results: Dict[str, List] = {}
        for (post_id, sort_name), result in zip(task_map, results):
            if post_id not in post_results:
                post_results[post_id] = []
            post_results[post_id].append(result)

        # Save to DB
        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        batch_new_comments = 0
        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for post_id, api_cc in batch:
            sort_results = post_results.get(post_id, [])
            post_new = 0

            # Skip posts where all 4 sorts returned 404
            if sort_results and all(isinstance(r, dict) and r.get("_deleted") for r in sort_results):
                continue

            for sort_result in sort_results:
                if sort_result is None:
                    continue
                if isinstance(sort_result, list):
                    comment_list = sort_result
                elif isinstance(sort_result, dict):
                    comment_list = sort_result.get("comments", [])
                else:
                    continue

                for c in comment_list:
                    if not c or not c.get("id"):
                        continue
                    if _save_flat_comment(cur, c, post_id):
                        post_new += 1
                        batch_new_comments += 1

            # Update velocity tracking (skip last_known_cc when api_cc=0, i.e. random posts)
            if api_cc > 0:
                cur.execute('''
                    UPDATE posts
                    SET last_comment_check_at = ?,
                        last_known_cc = ?
                    WHERE id = ?
                ''', (now_ts, api_cc, post_id))
            else:
                cur.execute('''
                    UPDATE posts SET last_comment_check_at = ? WHERE id = ?
                ''', (now_ts, post_id))

            # Compute velocity (exponential moving average)
            row = cur.execute(
                'SELECT comment_velocity, last_known_cc FROM posts WHERE id = ?',
                (post_id,)
            ).fetchone()
            if row and row[1] is not None:
                old_velocity = row[0] or 0.0
                new_velocity = post_new  # comments gained this fetch
                # EMA with alpha=0.3
                ema = 0.3 * new_velocity + 0.7 * old_velocity
                cur.execute('UPDATE posts SET comment_velocity = ? WHERE id = ?', (ema, post_id))

        conn.commit()
        conn.close()

        total_posts_processed += len(batch)
        total_comments_new += batch_new_comments
        elapsed = time.time() - t0

        logger.info("Comment fetcher: %d posts (%.1fs), %d new comments this batch, "
                     "%d total posts, %d total new comments, queue=%d",
                     len(batch), elapsed, batch_new_comments,
                     total_posts_processed, total_comments_new, queue.qsize())

    logger.info("Comment fetcher stopped: %d posts processed, %d new comments total",
                total_posts_processed, total_comments_new)


# ---------------------------------------------------------------------------
# Worker 3 — Background Scanner
# ---------------------------------------------------------------------------
async def background_scanner(db_path: str, queue: CommentQueue,
                             sessions: List[aiohttp.ClientSession],
                             shutdown: asyncio.Event,
                             listing_depth: int = 1000000,
                             interval: int = 1800):
    """Worker 3: Listing scan of newest N posts every ~interval seconds."""
    concurrency = max(len(sessions), 5)
    logger.info("Background scanner started (listing_workers=%d, listing_depth=%d, interval=%ds, sessions=%d)",
                concurrency, listing_depth, interval, len(sessions))

    cycle = 0
    while not shutdown.is_set():
        cycle += 1
        t0 = time.time()
        try:
            await _background_listing_scan(db_path, queue, sessions, shutdown,
                                           concurrency, cycle, listing_depth)
        except Exception as exc:
            logger.exception("Background scanner listing (cycle %d) error: %s", cycle, exc)

        elapsed = time.time() - t0
        logger.info("Background scanner cycle %d complete in %.1fs (%.1f min), queue=%d",
                     cycle, elapsed, elapsed / 60, queue.qsize())

        # Sleep until next cycle
        remaining = max(interval - elapsed, 10)
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=remaining)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Background scanner stopped")


async def _background_listing_scan(db_path: str, queue: CommentQueue,
                                   sessions: List[aiohttp.ClientSession],
                                   shutdown: asyncio.Event,
                                   concurrency: int, cycle: int,
                                   max_posts: int = 200000):
    """Listing scan via sort=new, limited to newest max_posts posts."""
    logger.info("Background scanner cycle %d: listing scan (concurrency=%d, max_posts=%d)",
                cycle, concurrency, max_posts)

    crawl_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    stored_counts = get_stored_comment_counts(db_path)

    snapshots: List[Tuple] = []
    post_rows: List[Tuple] = []
    agent_rows: List[Tuple] = []
    submolt_rows: List[Tuple] = []

    total_seen = 0
    total_deficit = 0
    total_pushed = 0
    page_size = 50
    sleep_between = 0.05 if _PROXIES else 0.7
    rate_limit_cooldown = 5.0 if _PROXIES else 60.0

    data_lock = asyncio.Lock()
    offset_lock = asyncio.Lock()
    next_offset = 0
    global_stop = False

    async def _worker(worker_id: int, session: aiohttp.ClientSession):
        nonlocal next_offset, global_stop, total_seen, total_deficit, total_pushed

        while True:
            if global_stop or shutdown.is_set():
                return

            async with offset_lock:
                if global_stop:
                    return
                if next_offset >= max_posts:
                    global_stop = True
                    return
                my_offset = next_offset
                next_offset += page_size

            data = await fetch_posts_page(session, my_offset, page_size, "new")

            if data is RATE_LIMITED:
                logger.info("  BG worker %d: rate limited at offset %d, cooling down",
                            worker_id, my_offset)
                await asyncio.sleep(rate_limit_cooldown)
                data = await fetch_posts_page(session, my_offset, page_size, "new")
                if data is RATE_LIMITED or data is None:
                    continue

            if data is None:
                logger.warning("BG worker %d: skipping failed page at offset %d",
                               worker_id, my_offset)
                continue

            posts = data.get("posts", [])
            if not posts:
                global_stop = True
                return

            # Collect deficit items from this page
            page_deficit: List[Tuple[str, int, float]] = []

            async with data_lock:
                for post in posts:
                    result = _process_post_data(post, crawl_ts, snapshots, post_rows,
                                                agent_rows, submolt_rows)
                    if result is None:
                        continue
                    pid, cc = result
                    total_seen += 1

                    stored = stored_counts.get(pid, 0)
                    deficit = cc - stored
                    if deficit > 0:
                        total_deficit += 1
                        if not queue.in_queue(pid):
                            priority = _compute_priority(deficit, cc, post.get("created_at"))
                            page_deficit.append((pid, cc, priority))

                if len(post_rows) >= 5000:
                    _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)
                    logger.info("  BG listing: %d posts scanned, %d deficit, %d pushed so far",
                                total_seen, total_deficit, total_pushed)
                    snapshots.clear()
                    post_rows.clear()
                    agent_rows.clear()
                    submolt_rows.clear()

            # Push deficits to queue immediately (outside data_lock)
            if page_deficit:
                added = await queue.push_batch(page_deficit)
                total_pushed += added

            if not data.get("has_more", False):
                global_stop = True
                return

            await asyncio.sleep(sleep_between)

    num_workers = concurrency if _PROXIES else 1
    workers = [asyncio.create_task(_worker(i, sessions[i % len(sessions)]))
               for i in range(num_workers)]
    await asyncio.gather(*workers)

    # Final flush
    if post_rows:
        _flush_phase1(db_path, snapshots, post_rows, agent_rows, submolt_rows)

    logger.info("BG listing cycle %d: %d seen, %d deficit, %d pushed to queue",
                 cycle, total_seen, total_deficit, total_pushed)


async def _background_random_sample(db_path: str, queue: CommentQueue,
                                    sessions: List[aiohttp.ClientSession],
                                    shutdown: asyncio.Event,
                                    detail_concurrency: int, cycle: int):
    """Random 5% sample of non-deleted posts, fetch details to get fresh comment_count."""
    conn = sqlite3.connect(db_path)
    all_ids = [r[0] for r in conn.execute('SELECT id FROM posts WHERE deleted_at IS NULL').fetchall()]
    conn.close()

    if not all_ids:
        logger.info("BG random sample cycle %d: no posts in DB", cycle)
        return

    sample_size = max(int(len(all_ids) * 0.05), 100)
    sample_ids = random.sample(all_ids, min(sample_size, len(all_ids)))

    logger.info("BG random sample cycle %d: checking %d posts (5%% of %d)",
                cycle, len(sample_ids), len(all_ids))

    stored_counts = get_stored_comment_counts(db_path)
    sem = asyncio.Semaphore(detail_concurrency)
    chunk_size = detail_concurrency  # match chunk to concurrency
    total_checked = 0
    total_deficit = 0
    total_pushed = 0

    for i in range(0, len(sample_ids), chunk_size):
        if shutdown.is_set():
            break

        chunk = sample_ids[i : i + chunk_size]
        tasks = [fetch_post_details(_next_session(sessions), pid, sem) for pid in chunk]
        results = await asyncio.gather(*tasks)

        chunk_deficit: List[Tuple[str, int, float]] = []
        deleted_ids: List[str] = []
        conn = sqlite3.connect(db_path, timeout=30)
        cur = conn.cursor()
        for pid, result in zip(chunk, results):
            total_checked += 1
            if result is None:
                continue

            # Mark deleted posts
            if result.get("_deleted"):
                deleted_ids.append(pid)
                continue

            if not result.get("success"):
                continue

            post_data = result.get("post", {})
            api_cc = post_data.get("comment_count", 0)
            stored = stored_counts.get(pid, 0)
            deficit = api_cc - stored

            # Save full agent profile from detail endpoint
            author = post_data.get("author")
            if author and author.get("id"):
                save_agent_full(cur, author)

            if deficit > 0 and not queue.in_queue(pid):
                priority = _compute_priority(deficit, api_cc, post_data.get("created_at"))
                chunk_deficit.append((pid, api_cc, priority))
                total_deficit += 1

        if deleted_ids:
            cur.executemany(
                'UPDATE posts SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?',
                [(pid,) for pid in deleted_ids],
            )
            logger.info("  BG random sample: marked %d posts as deleted", len(deleted_ids))
        conn.commit()
        conn.close()

        # Push immediately per chunk
        if chunk_deficit:
            added = await queue.push_batch(chunk_deficit)
            total_pushed += added
            logger.info("  BG random sample: pushed %d posts to queue (total %d/%d checked, %d deficit, %d pushed)",
                        added, total_checked, len(sample_ids), total_deficit, total_pushed)

        elif total_checked % 500 == 0:
            logger.info("  BG random sample: %d/%d checked, %d deficit, %d pushed so far",
                        total_checked, len(sample_ids), total_deficit, total_pushed)

    logger.info("BG random sample cycle %d: checked %d, %d deficit, %d pushed to queue",
                 cycle, total_checked, total_deficit, total_pushed)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def _partition_proxies(proxies: List[str], split: Tuple[int, int, int]
                       ) -> Tuple[List[str], List[str], List[str]]:
    """Split proxy list into 3 groups by ratio (discovery, fetcher, background).

    E.g. split=(20, 40, 40) with 50 proxies → 10, 20, 20.
    Falls back to even thirds if split doesn't work.
    """
    total = len(proxies)
    if total == 0:
        return [], [], []

    s = sum(split)
    n1 = max(1, round(total * split[0] / s))
    n2 = max(1, round(total * split[1] / s))
    n3 = max(1, total - n1 - n2)
    # Adjust if rounding overflows
    while n1 + n2 + n3 > total:
        if n3 > 1:
            n3 -= 1
        elif n2 > 1:
            n2 -= 1
        else:
            n1 -= 1

    shuffled = list(proxies)
    random.shuffle(shuffled)
    p1 = shuffled[:n1]
    p2 = shuffled[n1:n1 + n2]
    p3 = shuffled[n1 + n2:n1 + n2 + n3]
    return p1, p2, p3


async def main_async(args):
    shutdown = asyncio.Event()

    def _signal_handler():
        if shutdown.is_set():
            logger.warning("Second signal received — forcing exit")
            os._exit(1)
        logger.info("Shutdown signal received, finishing current work… (press Ctrl+C again to force quit)")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    init_db(args.db)

    queue = CommentQueue()

    # Partition proxies into dedicated pools per worker
    split = tuple(args.proxy_split)
    discovery_proxies, fetcher_proxies, background_proxies = _partition_proxies(_PROXIES, split)

    logger.info("Proxy partition: discovery=%d, fetcher=%d, background=%d",
                len(discovery_proxies), len(fetcher_proxies), len(background_proxies))

    discovery_sessions = await _create_session_pool(discovery_proxies)
    fetcher_sessions = await _create_session_pool(fetcher_proxies)
    all_sessions = [discovery_sessions, fetcher_sessions]

    tasks = [
        asyncio.create_task(
            discovery_scanner(args.db, queue, discovery_sessions, shutdown,
                              interval=args.discovery_interval,
                              depth=args.discovery_depth),
            name="discovery_scanner",
        ),
        asyncio.create_task(
            comment_fetcher(args.db, queue, fetcher_sessions, shutdown),
            name="comment_fetcher",
        ),
        asyncio.create_task(
            homepage_stats_loop(args.db, args.homepage_interval, shutdown),
            name="homepage_stats",
        ),
        asyncio.create_task(
            queue_stats_loop(queue, shutdown, interval=30),
            name="queue_stats",
        ),
    ]

    if not args.no_background:
        background_sessions = await _create_session_pool(background_proxies)
        all_sessions.append(background_sessions)
        tasks.append(asyncio.create_task(
            background_scanner(args.db, queue, background_sessions, shutdown,
                              listing_depth=args.listing_depth,
                              interval=args.background_interval),
            name="background_scanner",
        ))
    else:
        logger.info("Background scanner DISABLED (--no-background)")

    try:
        await asyncio.gather(*tasks)
    except Exception:
        logger.exception("Unhandled exception in worker")
        shutdown.set()
    finally:
        for pool in all_sessions:
            await _close_session_pool(pool)

    logger.info("All workers stopped. Exiting.")


def main():
    parser = argparse.ArgumentParser(
        description="Moltbook continuous DB maintenance daemon v3 (3-worker concurrent crawler)",
    )
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="Path to SQLite database (default: %(default)s)")
    parser.add_argument("--proxies", type=str, default=None,
                        help="Path to proxy list file (one URL per line)")
    parser.add_argument("--proxy-split", type=int, nargs=3, default=[20, 70, 10],
                        metavar=("DISC", "FETCH", "BG"),
                        help="Proxy %% split: discovery fetcher background (default: 20 70 10)")
    parser.add_argument("--discovery-interval", type=int, default=150,
                        help="Seconds between discovery scans (default: 150)")
    parser.add_argument("--discovery-depth", type=int, default=10000,
                        help="Posts per sort+time combo in discovery (default: 10000)")
    parser.add_argument("--homepage-interval", type=int, default=1800,
                        help="Seconds between homepage stats fetches (default: 1800)")
    parser.add_argument("--listing-depth", type=int, default=1000000,
                        help="Max posts to scan in background listing (default: 1000000)")
    parser.add_argument("--background-interval", type=int, default=1800,
                        help="Seconds between background listing scans (default: 1800)")
    parser.add_argument("--no-background", action="store_true",
                        help="Disable background scanner (run only discovery + fetcher)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    # Load proxies
    if args.proxies:
        proxy_path = args.proxies
        if not os.path.isfile(proxy_path):
            logger.error("Proxy file not found: %s", proxy_path)
            sys.exit(1)
        with open(proxy_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http://") or line.startswith("https://"):
                    _PROXIES.append(line)
                else:
                    parts = line.split(":")
                    if len(parts) == 4:
                        host, port, user, pwd = parts
                        _PROXIES.append(f"http://{user}:{pwd}@{host}:{port}")
                    elif len(parts) == 2:
                        _PROXIES.append(f"http://{line}")
                    else:
                        logger.warning("Unrecognized proxy format, using as-is: %s", line)
                        _PROXIES.append(line)
        if not _PROXIES:
            logger.error("No proxies found in %s", proxy_path)
            sys.exit(1)
        logger.info("Loaded %d proxies from %s", len(_PROXIES), proxy_path)

    logger.info("=" * 60)
    logger.info("Moltbook DB Maintenance v3 (3-Worker Concurrent Crawler)")
    logger.info("  db=%s  proxies=%d", args.db, len(_PROXIES))
    logger.info("  proxy_split=%s (discovery/fetcher/background %%)",
                args.proxy_split)
    logger.info("  discovery_interval=%ds  discovery_depth=%d",
                args.discovery_interval, args.discovery_depth)
    logger.info("  homepage_interval=%ds", args.homepage_interval)
    logger.info("=" * 60)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
