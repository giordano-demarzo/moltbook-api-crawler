#!/usr/bin/env python3
"""Test API endpoint concurrency limits."""
import asyncio
import itertools
import sqlite3
import time
import sys

import aiohttp
from aiohttp_socks import ProxyConnector

API_BASE = "https://www.moltbook.com/api/v1"
HEADERS = {
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}

# Load proxies
PROXIES = []
with open("proxies.txt") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) == 4:
            host, port, user, pwd = parts
            PROXIES.append(f"http://{user}:{pwd}@{host}:{port}")
        else:
            PROXIES.append(line)

print(f"Loaded {len(PROXIES)} proxies")

# Load test data
conn = sqlite3.connect("data/moltbook.db")
POST_IDS = [r[0] for r in conn.execute("SELECT id FROM posts WHERE comment_count > 5 ORDER BY RANDOM() LIMIT 500").fetchall()]
AGENT_NAMES = [r[0] for r in conn.execute("SELECT name FROM agents WHERE name IS NOT NULL ORDER BY RANDOM() LIMIT 200").fetchall()]
conn.close()

_counter = itertools.count()


async def _make_session(proxy: str) -> aiohttp.ClientSession:
    connector = ProxyConnector.from_url(proxy)
    return aiohttp.ClientSession(connector=connector, headers=HEADERS)


async def _test_single(session: aiohttp.ClientSession, url: str):
    """Fire a single request, return (status, latency_ms)."""
    t0 = time.monotonic()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            await resp.read()
            latency = (time.monotonic() - t0) * 1000
            return resp.status, latency
    except Exception as e:
        latency = (time.monotonic() - t0) * 1000
        return f"ERR:{type(e).__name__}", latency


async def test_endpoint(name: str, urls: list, concurrency_levels: list):
    """Test an endpoint at various concurrency levels."""
    print(f"\n{'='*70}")
    print(f"  Endpoint: {name}")
    print(f"  Sample URL: {urls[0]}")
    print(f"  URLs available: {len(urls)}")
    print(f"{'='*70}")
    print(f"  {'Conc':>6}  {'Reqs':>5}  {'OK':>5}  {'429':>5}  {'5xx':>5}  {'Err':>5}  {'Avg ms':>8}  {'p50 ms':>8}  {'p95 ms':>8}  {'RPS':>7}")
    print(f"  {'-'*6}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*7}")

    for conc in concurrency_levels:
        # Create sessions (one per proxy, round-robin)
        sessions = [await _make_session(PROXIES[i % len(PROXIES)]) for i in range(min(conc, len(PROXIES)))]

        num_reqs = min(conc * 2, len(urls))  # enough to measure, not too many
        test_urls = urls[:num_reqs]

        sem = asyncio.Semaphore(conc)

        async def _do(url, idx):
            async with sem:
                s = sessions[idx % len(sessions)]
                return await _test_single(s, url)

        t0 = time.monotonic()
        results = await asyncio.gather(*[_do(u, i) for i, u in enumerate(test_urls)])
        wall_time = time.monotonic() - t0

        # Tally
        ok = sum(1 for s, _ in results if s == 200)
        r429 = sum(1 for s, _ in results if s == 429)
        r5xx = sum(1 for s, _ in results if isinstance(s, int) and 500 <= s < 600)
        errs = sum(1 for s, _ in results if isinstance(s, str))
        latencies = sorted([lat for _, lat in results])
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        p50 = latencies[len(latencies) // 2] if latencies else 0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
        rps = num_reqs / wall_time if wall_time > 0 else 0

        print(f"  {conc:>6}  {num_reqs:>5}  {ok:>5}  {r429:>5}  {r5xx:>5}  {errs:>5}  {avg_lat:>8.0f}  {p50:>8.0f}  {p95:>8.0f}  {rps:>7.1f}")

        for s in sessions:
            await s.close()

        # If we got mostly errors, no point testing higher
        if (r429 + r5xx + errs) > num_reqs * 0.8:
            print(f"  ** Stopping: too many errors at concurrency {conc} **")
            break

        # Brief pause between levels
        await asyncio.sleep(2)


async def main():
    concurrency_levels = [10, 25, 50, 100, 200, 500]

    # 1. Posts listing
    listing_urls = [f"{API_BASE}/posts?limit=50&offset={i*50}&sort=new" for i in range(500)]
    await test_endpoint("Posts listing (/posts?sort=new)", listing_urls, concurrency_levels)

    await asyncio.sleep(3)

    # 2. Post details
    detail_urls = [f"{API_BASE}/posts/{pid}" for pid in POST_IDS]
    await test_endpoint("Post details (/posts/{id})", detail_urls, concurrency_levels)

    await asyncio.sleep(3)

    # 3. Post comments (sort=new)
    comment_urls = [f"{API_BASE}/posts/{pid}/comments?sort=new&limit=100" for pid in POST_IDS]
    await test_endpoint("Post comments (/posts/{id}/comments)", comment_urls, concurrency_levels)

    await asyncio.sleep(3)

    # 4. Agent profiles
    agent_urls = [f"{API_BASE}/agents/profile?name={name}" for name in AGENT_NAMES]
    await test_endpoint("Agent profiles (/agents/profile)", agent_urls, concurrency_levels)

    await asyncio.sleep(3)

    # 5. Submolts
    submolt_urls = [f"{API_BASE}/submolts?limit=100&offset={i*100}" for i in range(200)]
    await test_endpoint("Submolts (/submolts)", submolt_urls, concurrency_levels)

    await asyncio.sleep(3)

    # 6. Homepage
    homepage_urls = [f"{API_BASE}/homepage?shuffle={int(time.time()*1000)+i}" for i in range(500)]
    await test_endpoint("Homepage (/homepage)", homepage_urls, concurrency_levels)


if __name__ == "__main__":
    asyncio.run(main())
