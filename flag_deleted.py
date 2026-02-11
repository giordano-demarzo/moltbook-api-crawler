#!/usr/bin/env python3
"""
One-off script: paginate the full /posts listing (sort=new) to collect all
live post IDs, then flag every post in our DB that wasn't found as deleted.
"""
import argparse
import asyncio
import itertools
import sqlite3
import time

import aiohttp
from aiohttp_socks import ProxyConnector

API_BASE = "https://www.moltbook.com/api/v1"
HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}

_proxies = []
_proxy_counter = itertools.count()


def _next_proxy():
    if not _proxies:
        return None
    return _proxies[next(_proxy_counter) % len(_proxies)]


def _create_session():
    proxy = _next_proxy()
    if proxy:
        connector = ProxyConnector.from_url(proxy)
        return aiohttp.ClientSession(connector=connector)
    return aiohttp.ClientSession()


async def fetch_page(session, offset, limit=50, retries=5):
    url = f"{API_BASE}/posts?limit={limit}&offset={offset}&sort=new"
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 429:
                    print(f"  429 at offset {offset}, sleeping 10s")
                    await asyncio.sleep(10)
                    continue
                data = await resp.json()
                return data
        except Exception as e:
            if attempt == retries:
                print(f"  Failed offset {offset} after {retries} attempts: {e}")
                return None
            await asyncio.sleep(2)
    return None


async def main(db_path: str, concurrency: int):
    live_ids = set()
    page_size = 50
    offset = 0
    t0 = time.time()

    # One session per proxy (or single direct session)
    num_sessions = max(len(_proxies), 1)
    sessions = [_create_session() for _ in range(num_sessions)]

    try:
        while True:
            tasks = []
            offsets = []
            for i in range(concurrency):
                session = sessions[i % num_sessions]
                tasks.append(fetch_page(session, offset + i * page_size))
                offsets.append(offset + i * page_size)

            results = await asyncio.gather(*tasks)

            stop = False
            for off, data in zip(offsets, results):
                if data is None:
                    continue
                posts = data.get("posts", [])
                if not posts:
                    stop = True
                    break
                for p in posts:
                    live_ids.add(p["id"])
                if not data.get("has_more", False):
                    stop = True
                    break

            offset += concurrency * page_size

            if offset % 10000 < concurrency * page_size:
                elapsed = time.time() - t0
                print(f"  Scanned offset {offset:,}, live posts found: {len(live_ids):,} ({elapsed:.0f}s)")

            if stop:
                break
    finally:
        for s in sessions:
            await s.close()

    elapsed = time.time() - t0
    print(f"\nListing complete: {len(live_ids):,} live posts found in {elapsed:.0f}s")

    # Compare against DB
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()

    db_ids = set(r[0] for r in cur.execute("SELECT id FROM posts").fetchall())
    already_deleted = set(r[0] for r in cur.execute(
        "SELECT id FROM posts WHERE deleted_at IS NOT NULL"
    ).fetchall())

    not_in_listing = db_ids - live_ids
    newly_deleted = not_in_listing - already_deleted

    print(f"Posts in DB: {len(db_ids):,}")
    print(f"Already flagged deleted: {len(already_deleted):,}")
    print(f"Not found in listing: {len(not_in_listing):,}")
    print(f"Newly deleted (to flag): {len(newly_deleted):,}")

    if newly_deleted:
        cur.executemany(
            "UPDATE posts SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
            [(pid,) for pid in newly_deleted],
        )
        conn.commit()
        print(f"Flagged {len(newly_deleted):,} posts as deleted.")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flag deleted posts by comparing against API listing")
    parser.add_argument("--db", default="data/moltbook.db", help="Path to SQLite DB")
    parser.add_argument("--proxies", default=None, help="Path to proxy list file")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent page fetches")
    args = parser.parse_args()

    if args.proxies:
        with open(args.proxies) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http://") or line.startswith("https://"):
                    _proxies.append(line)
                else:
                    parts = line.split(":")
                    if len(parts) == 4:
                        host, port, user, pwd = parts
                        _proxies.append(f"http://{user}:{pwd}@{host}:{port}")
                    elif len(parts) == 2:
                        _proxies.append(f"http://{line}")
                    else:
                        _proxies.append(line)
        print(f"Loaded {len(_proxies)} proxies")

    asyncio.run(main(args.db, args.concurrency))
