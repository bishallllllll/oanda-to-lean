#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

API_HOST = "https://api-fxpractice.oanda.com"
TOKEN_FILE = os.environ.get(
    "OANDA_TOKEN_FILE",
    os.path.expanduser("~/.openalice/provider-keys.json"),
)
CH_DB = "oanda"

DERIVED = {
    "M2": "INTERVAL 2 MINUTE", "M4": "INTERVAL 4 MINUTE", "M5": "INTERVAL 5 MINUTE",
    "M10": "INTERVAL 10 MINUTE", "M15": "INTERVAL 15 MINUTE", "M30": "INTERVAL 30 MINUTE",
    "H1": "INTERVAL 1 HOUR", "H2": "INTERVAL 2 HOUR", "H3": "INTERVAL 3 HOUR",
    "H4": "INTERVAL 4 HOUR", "H6": "INTERVAL 6 HOUR", "H8": "INTERVAL 8 HOUR",
    "H12": "INTERVAL 12 HOUR", "D": "INTERVAL 1 DAY",
}
NATIVE = ["W", "M"]


def get_token():
    env = os.environ.get("OANDA_TOKEN")
    if env:
        return env.strip()
    with open(TOKEN_FILE) as f:
        return json.load(f)["oanda"]


def api_get(path, token, params=None):
    for attempt in range(8):
        r = requests.get(
            f"{API_HOST}{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        if r.status_code == 429:
            time.sleep(min(float(r.headers.get("Retry-After", 1)) * (attempt + 1), 30))
            continue
        r.raise_for_status()
        return r.json()
    # Used to fall out of the loop returning None, which the caller subscripted into
    # a TypeError. OANDA practice sends `Retry-After: 0`, so all 8 attempts can burn in
    # milliseconds -- and now that a failure turns the job red, fail loudly instead.
    raise RuntimeError(f"rate-limited after {attempt + 1} attempts: {path}")


def ch_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["CH_HOST"],
        username=os.environ["CH_USER"],
        password=os.environ["CH_PASSWORD"],
        port=int(os.environ.get("CH_PORT", "8443")),
        secure=True,
    )


def complete_instruments(cli):
    rows = cli.query("SELECT instrument FROM oanda.ingest_status WHERE complete=1").result_rows
    return {r[0] for r in rows}


def _utc_naive(ts):
    """Normalise any datetime to a naive UTC wall clock for ClickHouse string literals.

    clickhouse_connect's default naive_datetime_insert="local" interprets wall-clock
    strings in the *runner's* timezone, so a runner that is not UTC would silently
    shift every bound. Pinning to UTC here makes the value correct anywhere.
    """
    if isinstance(ts, str):
        ts = dt.datetime.fromisoformat(ts)
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts


def watermarks(cli, granularity):
    """Latest derived bar start per instrument for one granularity.

    The watermark is read from the DATA, not from the `materialized` marker table.
    `materialized.done_at` records when a granularity was materialised, not how far
    the data reached, so treating it as a watermark would silently skip any bar that
    arrived after the marker was written. `oanda.candles` is
    ORDER BY (granularity, instrument, ts), so this is a primary-key range scan.

    Returns {instrument: max_ts_or_None}.
    """
    rows = cli.query(
        "SELECT instrument, max(ts) FROM oanda.candles "
        f"WHERE granularity = '{granularity}' GROUP BY instrument"
    ).result_rows
    return {r[0]: r[1] for r in rows}


def derive(cli, instrument, granularity, interval, since=None):
    """Aggregate candles_m1 into one granularity.

    `since` restricts to M1 rows at or after that instant, so each run only
    re-derives the last bucket (which may have been partial last time) plus
    everything since. Omitting it derives the full history (bootstrap path).
    """
    bucket = f"toStartOfInterval(ts, {interval}, toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))"
    bound = "" if since is None else f"  AND ts >= '{since}'\n"
    sql = f"""
INSERT INTO oanda.candles (granularity, instrument, ts, open, high, low, close, volume)
SELECT '{granularity}', instrument,
       {bucket},
       argMin(open, ts), max(high), min(low), argMax(close, ts), sum(volume)
FROM oanda.candles_m1 FINAL
WHERE instrument = '{instrument}'
{bound}GROUP BY instrument, {bucket}
"""
    cli.command(sql)


def fetch_native(token, cli, instrument, granularity, since=None, full=False):
    """Pull native W/M bars from the OANDA API.

    `since` resumes from the existing watermark instead of re-pulling 24 years on
    every run. It backs off by the bar length so the currently-forming bar is
    re-fetched rather than assumed complete (the API is called with
    includeIncomplete=false, so the newest bar only appears once it closes).
    `--full` forces the full-history path for recovery.
    """
    rows = []
    back = {"W": 8, "M": 32}.get(granularity, 8)
    if full or since is None:
        from_ts = dt.datetime.utcnow() - dt.timedelta(days=365 * 24)
    else:
        from_ts = _utc_naive(since) - dt.timedelta(days=back)
    while True:
        data = api_get(
            f"/v3/instruments/{instrument}/candles", token,
            params={
                "granularity": granularity, "price": "M", "count": 5000,
                "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "includeIncomplete": "false",
            },
        )
        got = len(data["candles"])
        if got:
            for c in data["candles"]:
                rows.append(
                    (instrument, granularity, c["time"][0:23].replace("T", " "),
                     float(c["mid"]["o"]), float(c["mid"]["h"]), float(c["mid"]["l"]),
                     float(c["mid"]["c"]), int(c["volume"]))
                )
        if got < 5000:
            break
        from_ts = dt.datetime.fromisoformat(
            data["candles"][-1]["time"].replace("Z", "+00:00")
        ).replace(tzinfo=None) + dt.timedelta(days=1)
        time.sleep(0.35)
    if rows:
        cli.insert(
            "oanda.candles", rows,
            column_names=["instrument", "granularity", "ts", "open", "high", "low", "close", "volume"],
        )
    return len(rows)


def materialize_one(token, instrument, marks=None, full=False):
    """Derive/refresh every resolution for one instrument, incrementally.

    The watermark per granularity comes from oanda.candles itself, so a lost or
    stale marker can never cause a permanent skip -- which is exactly how the
    previous marker-based version silently froze every higher timeframe.
    `marks` is the precomputed {granularity: {instrument: max_ts}} mapping.
    """
    cli = ch_client()

    def mark_for(g):
        return None if full or not marks else marks.get(g, {}).get(instrument)

    try:
        done = []
        for g, interval in DERIVED.items():
            since = mark_for(g)
            since_str = None if since is None else _utc_naive(since).strftime("%Y-%m-%d %H:%M:%S")
            derive(cli, instrument, g, interval, since_str)
            # Still logged, as an ops record only. Nothing gates on it: the watermark
            # is read from oanda.candles. done_at is when materialisation ran, NOT how
            # far the data reached -- treating it as a watermark is what froze the HTFs.
            cli.command(
                f"INSERT INTO oanda.materialized FORMAT CSV\n{instrument},{g},0,"
                f"{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            done.append(g)
        for g in NATIVE:
            n = fetch_native(token, cli, instrument, g, mark_for(g), full=full)
            cli.command(
                f"INSERT INTO oanda.materialized FORMAT CSV\n{instrument},{g},{n},"
                f"{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            done.append(f"{g}({n})")
        return instrument, done, None
    except Exception as exc:
        return instrument, [], repr(exc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["composio", "direct"], default="direct")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--axis", type=int, default=None)
    ap.add_argument("--axes", type=int, default=1)
    ap.add_argument("--instruments", nargs="*", default=None)
    ap.add_argument(
        "--full", action="store_true",
        help="Ignore watermarks and re-derive the full history (recovery path).",
    )
    args = ap.parse_args()

    token = get_token()
    cli = ch_client()
    pool = complete_instruments(cli)
    if args.instruments:
        pool = {i for i in args.instruments}
    instruments = sorted(pool)
    if args.axis is not None:
        instruments = [i for idx, i in enumerate(instruments) if idx % args.axes == args.axis]
    print(f"materializable={len(instruments)} axis={args.axis}/{args.axes} full={args.full}", flush=True)
    if not instruments:
        print("AXIS_DONE", flush=True)
        return

    # One watermark query per granularity, reused by every instrument. Safe under the
    # threadpool because each instrument is handled by exactly one worker, so the
    # precomputed value for that instrument is still correct when it runs.
    marks = {} if args.full else {g: watermarks(cli, g) for g in list(DERIVED) + list(NATIVE)}

    failures = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {
            ex.submit(materialize_one, token, inst, marks, args.full): inst
            for inst in instruments
        }
        for fut in as_completed(futures):
            instrument, done, err = fut.result()
            print(f"{instrument} done={done} err={err}", flush=True)
            if err:
                failures.append(instrument)

    # Non-zero exit when anything failed. Previously every instrument error was
    # swallowed here and main() always returned 0, so a run in which all 121
    # instruments failed still looked successful to the workflow.
    if failures:
        print(f"FAILED {len(failures)}/{len(instruments)}: {sorted(failures)}", flush=True)
        sys.exit(1)

    check_lag(cli, instruments, marks)


def check_lag(cli, instruments, marks):
    """Fail the run when a derived bar lags the M1 frontier it is built from.

    This is the watchdog the original incident lacked. A run where M1 ingest is
    silently broken derives nothing, raises nothing, and looks identical to a healthy
    one -- which is exactly how every higher timeframe stayed frozen behind a green
    build for five weeks. W and M are excluded: their bars are native OANDA periods and
    legitimately lag a completed daily bar.
    """
    frontier = cli.query("SELECT max(ts) FROM oanda.candles_m1").result_rows[0][0]
    if frontier is None:
        print("::warning::candles_m1 is empty - cannot verify derived-timeframe lag")
        return
    # Tolerance per granularity, in seconds: one bar, or ~2 days for D.
    tolerance = {"D": 2 * 86400}
    stale = []
    for g in DERIVED:
        if not marks.get(g):
            continue  # nothing derived yet for anyone; not a lag signal
        newest = max((marks[g].get(i) for i in instruments if marks[g].get(i)), default=None)
        if newest is None:
            continue
        age = (frontier - newest).total_seconds()
        limit = tolerance.get(g, 3600)
        if age > limit:
            stale.append(f"{g} lags {age / 3600:.1f}h (limit {limit / 3600:.1f}h)")
    if stale:
        for s in stale:
            print(f"::warning::derived-timeframe lag: {s}")
        print(f"STALE_DERIVED {len(stale)}/{len(DERIVED)} granularities behind candles_m1")
        sys.exit(1)
    print(f"lag OK: all {len(DERIVED)} derived granularities within tolerance")


if __name__ == "__main__":
    main()
