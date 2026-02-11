# Moltbook Crawler & Analysis

Code for crawling [Moltbook](https://www.moltbook.com) (a Reddit-style platform for AI agents) and reproducing the analyses in:

> **Collective Behavior of AI Agents on Moltbook**
> Giordano De Marzo (2026)

The full dataset is available on [HuggingFace](https://huggingface.co/datasets/giordano-dm/moltbook-crawl).

## Repository Structure

```
maintain_db_v3.py              # Concurrent 3-worker crawler (recommended)
maintain_db.py                 # Legacy sequential crawler (v1)
analysis_scripts/              # Scripts to generate paper figures
  figure1_growth.py            # Platform growth over time
  figure2_distributions.py     # Heavy-tailed distributions (comments/post, posts/submolt, subscribers)
  figure3_popularity.py        # Post popularity metrics (upvotes vs tree size)
  figure4_structure.py         # Discussion tree structure (depth vs width)
  figure5_decay.py             # Temporal dynamics of discussions
  figure_style.py              # Shared plotting configuration
latex/                         # Paper source files
```

## Installation

```bash
pip install -r requirements.txt
```

## Crawler (v3)

`maintain_db_v3.py` is the main crawler -- a concurrent async daemon that runs 3 workers simultaneously to keep up with the platform's growth (~400K posts/week). It uses proxy rotation and priority-based scheduling to maximize comment capture.

### Architecture

The crawler runs 3 concurrent workers sharing a priority queue:

**Worker 1 -- Discovery Scanner** (every ~2.5 min)
- Scans 11 sort+time combinations (`top`, `comments`, `new` across `hour/day/week/month/all`)
- Detects posts with missing comments (deficit) by comparing API counts against stored counts
- Pushes deficit posts to the shared priority queue
- Saves post data, snapshots, agent info, and submolt info

**Worker 2 -- Comment Fetcher** (continuous)
- Pulls posts from the priority queue
- Fetches comments via 4 sort endpoints (`old`, `new`, `top`, `controversial`) per post
- Saves comments and updates velocity tracking
- When the queue is empty, refreshes stale agent profiles (not updated in 24h) via the `/agents/profile` endpoint, harvesting bonus comments and posts from agent activity

**Worker 3 -- Background Scanner** (~60 min cycles)
- Listing scan: paginates through the newest 200K posts via `sort=new`, saving post data and pushing deficits to the queue
- Random sample: checks 5% of all non-deleted posts via the detail endpoint to detect missed comment growth
- Marks posts returning 404 as deleted so they are excluded from future scans

Additionally, a background task fetches platform-wide homepage stats every 30 minutes.

### Priority Queue

Posts are prioritized in the queue by urgency (lower = fetched first):
- **Deficit size**: posts missing more comments are fetched first
- **Comment activity**: posts with >100 comments get a boost
- **Recency**: posts less than 1 hour old get the highest boost

### Deleted Post Handling

The Moltbook platform regularly deletes spam/bot posts. The crawler detects 404 responses and marks posts as deleted (`deleted_at` timestamp), excluding them from future random sampling and comment fetching. This avoids wasting requests on ~480K+ deleted posts.

### Quick Start

```bash
# Basic usage with proxy rotation
python maintain_db_v3.py --proxies proxies.txt

# With verbose logging
python maintain_db_v3.py --proxies proxies.txt -v

# Without the background scanner
python maintain_db_v3.py --proxies proxies.txt --no-background -v
```

The proxy file should contain one proxy URL per line (e.g., `socks5://user:pass@host:port`).

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `data/moltbook.db` | Path to SQLite database |
| `--proxies` | none | Path to proxy list file (one URL per line) |
| `--proxy-split D F B` | `20 70 10` | Proxy % split across discovery, fetcher, background |
| `--discovery-interval` | 150 | Seconds between discovery scans |
| `--discovery-depth` | 5000 | Posts per sort+time combo in discovery |
| `--listing-depth` | 200000 | Max posts to scan in background listing |
| `--homepage-interval` | 1800 | Seconds between homepage stats fetches |
| `--no-background` | off | Disable background scanner entirely |
| `-v` | off | Verbose (DEBUG) logging |

### Proxy Configuration

The crawler partitions proxies into dedicated pools per worker to avoid contention. The default split allocates 20% to discovery, 70% to the comment fetcher, and 10% to the background scanner. For example, with 50 proxies: 10 for discovery, 35 for the fetcher, 5 for background.

### API Notes

- The Moltbook API returns a maximum of 100 comments per sort endpoint, so at most ~400 comments can be retrieved per post across the 4 sorts. Posts with more comments will have gaps.
- The API uses `sort=comments` (not `sort=discussed`) for sorting by comment count with time filtering.
- Posts deleted from the platform return HTTP 404; these are detected and excluded from future scans.
- No rate limiting (HTTP 429) has been observed even at 200+ concurrent requests with proxies.

## Analysis Scripts

Each script generates one figure from the paper. All accept a `--db` flag to specify the database path:

```bash
cd analysis_scripts
python figure1_growth.py --db ../data/moltbook_paper_till_feb8.db
python figure2_distributions.py --db ../data/moltbook_paper_till_feb8.db
python figure3_popularity.py --db ../data/moltbook_paper_till_feb8.db
python figure4_structure.py --db ../data/moltbook_paper_till_feb8.db
python figure5_decay.py --db ../data/moltbook_paper_till_feb8.db
```

To reproduce the paper figures exactly, use data up to and including February 8, 2026. The analysis database used in the paper is available on the [HuggingFace dataset page](https://huggingface.co/datasets/giordano-dm/moltbook-crawl).

## Database Schema

The SQLite database contains the following tables:

- **posts** -- post metadata (title, content, author, submolt, votes, timestamps, deletion status)
- **comments** -- comment trees (content, author, parent, depth, votes)
- **agents** -- agent profiles (name, description, karma, X/Twitter info, avatar, creation date)
- **submolts** -- community metadata (name, description, subscriber count)
- **post_snapshots** -- time series of post vote/comment counts
- **agent_snapshots** -- time series of agent karma/follower counts
- **submolt_snapshots** -- time series of submolt subscriber counts
- **homepage_stats** -- platform-wide aggregate counts over time

See the [HuggingFace dataset card](https://huggingface.co/datasets/giordano-dm/moltbook-crawl) for full schema details.

## License

MIT
