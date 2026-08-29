# oanda-to-lean

Non-stop OANDA practice M1 candle accumulation into free-tier ClickHouse Cloud, driven by a free GitHub Actions cron — no machine needs to stay on.

## Architecture

```
GH Actions cron (hourly) ──► puller.py ──► OANDA v20 practice API (5k-candle pages)
      │                            │
      │ checkpoints (ingest_status)│ candlesticks
      ▼                            ▼
                      ClickHouse Cloud (oanda.candles_m1)
```

- **State lives in ClickHouse** (`oanda.ingest_status`, one row per instrument, `last_ts` after every page) so every cron run resumes exactly where the previous one stopped — runners are ephemeral and runs are cancel-safe.
- **Dedupe-safe**: `ReplacingMergeTree` ordered by `(instrument, ts)`; a cancelled mid-insert run that re-runs cannot double rows.
- **Budgeted runs**: each job stops cleanly at 40 minutes (`--max-minutes`), so the hourly schedule never overlaps.

## Schema (ClickHouse)

```sql
CREATE DATABASE IF NOT EXISTS oanda;
CREATE TABLE oanda.candles_m1 (
  instrument LowCardinality(String),
  ts DateTime64(3, 'UTC'),
  open Float64, high Float64, low Float64, close Float64,
  volume UInt64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (instrument, ts);

CREATE TABLE oanda.ingest_status (
  instrument String,
  granularity String,
  last_ts DateTime64(3, 'UTC'),
  updated_at DateTime('UTC')
) ENGINE = MergeTree()
ORDER BY (instrument, granularity);
```

M1 is the canonical store (~2.6 M rows per instrument for 5 years); H1/H4/D/W/M are derived on query with `date_bin()`.

## Secrets (GitHub Actions)

| Secret | Value |
|---|---|
| `OANDA_TOKEN` | OANDA practice v20 bearer token |
| `CH_HOST` | ClickHouse Cloud host |
| `CH_PORT` | 8443 |
| `CH_USER` | default |
| `CH_PASSWORD` | ClickHouse Cloud password |

## Usage

```bash
# full backfill, all instruments, 5 years, 40-min budget
python3 puller/puller.py --mode direct --max-minutes 40

# resume (no-op if caught up) — checkpoint read from ClickHouse
python3 puller/puller.py --mode direct

# manual window
python3 puller/puller.py --mode direct --instruments EUR_USD --since 2026-08-27

# watch fill
SELECT instrument, count(), min(ts), max(ts)
FROM oanda.candles_m1 GROUP BY instrument ORDER BY instrument;
```

## Costs

- GitHub Actions: within the 2,000-3,000 free private-repo minutes/month (backfill ~700 one-time, then ~30-60 min/month incremental).
- ClickHouse Cloud free tier (~1 TB columnar; hourly status upserts double as keep-alive).
- OANDA practice API: free demo token. Note: no historical tick data — candles only, M1 depth ~5 years.
