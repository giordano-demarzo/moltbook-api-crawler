# Moltbook Crawler & Analysis

Code for crawling [Moltbook](https://www.moltbook.com) (a Reddit-style platform for AI agents) and reproducing the analyses in:

> **Collective Behavior of AI Agents on Moltbook**
> Giordano De Marzo (2026)

The full dataset is available on [HuggingFace](https://huggingface.co/datasets/giordano-dm/moltbook-crawl).

## Repository Structure

```
maintain_db.py              # Async crawler / DB maintenance daemon
analysis_scripts/           # Scripts to generate paper figures
  figure1_growth.py         # Platform growth over time
  figure2_distributions.py  # Heavy-tailed distributions (comments/post, posts/submolt, subscribers)
  figure3_popularity.py     # Post popularity metrics (upvotes vs tree size)
  figure4_structure.py      # Discussion tree structure (depth vs width)
  figure5_decay.py          # Temporal dynamics of discussions
  figure_style.py           # Shared plotting configuration
latex/                      # Paper source files
```

## Installation

```bash
pip install -r requirements.txt
```

## Crawler

`maintain_db.py` is an async daemon that continuously crawls the Moltbook API and stores data in a local SQLite database. It runs three phases per cycle:

1. **Listing scan** -- paginate through all posts, record snapshots, detect comment-count changes
2. **Detail fetch** -- fetch full discussion trees for posts with new comments
3. **Profile & submolt sweep** -- refresh stale agent profiles and all submolt metadata

### Creating a database from scratch

```bash
python maintain_db.py --db data/moltbook.db
```

This initializes all tables automatically and runs a single full cycle (all three phases). The resulting database has the same schema as the one on HuggingFace.

### Running as a daemon

```bash
python maintain_db.py --db data/moltbook.db --loop --interval 3600
```

This runs continuously, executing a full cycle every hour and collecting homepage stats every 30 minutes. Stop gracefully with `Ctrl+C` (SIGINT).

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | `data/moltbook.db` | Path to SQLite database |
| `--phase 1/2/3` | all | Run specific phase(s); can be repeated |
| `--loop` | off | Run continuously in daemon mode |
| `--interval` | 3600 | Seconds between cycles (daemon mode) |
| `--concurrency` | 5 | Concurrent requests for listing scan |
| `--detail-concurrency` | 20 | Concurrent requests for detail fetch |
| `--agent-batch` | 5000 | Max agents per sweep |
| `--agent-max-age` | 12.0 | Hours before an agent profile is considered stale |
| `--homepage-stats` | off | Enable homepage stats collection (auto in `--loop`) |
| `--homepage-interval` | 1800 | Seconds between homepage stats fetches |
| `-v` | off | Verbose (DEBUG) logging |

### API rate limiting

The crawler handles HTTP 429 responses by cooling down for 60 seconds before retrying. The Moltbook API limits comment retrieval to 100 comments per request; posts with more comments will have only the first 100 stored.

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

- **posts** -- post metadata (title, content, author, submolt, votes, timestamps)
- **comments** -- comment trees (content, author, parent, depth, votes)
- **agents** -- agent profiles (name, description, karma, X/Twitter info)
- **submolts** -- community metadata (name, description, subscriber count)
- **post_snapshots** -- time series of post vote/comment counts
- **agent_snapshots** -- time series of agent karma/follower counts
- **submolt_snapshots** -- time series of submolt subscriber counts
- **homepage_stats** -- platform-wide aggregate counts over time

See the [HuggingFace dataset card](https://huggingface.co/datasets/giordano-dm/moltbook-crawl) for full schema details.

## License

MIT
