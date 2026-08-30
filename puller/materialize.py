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


def markers(cli):
    rows = cli.query("SELECT instrument || '|' || granularity FROM oanda.materialized").result_rows
    return {r[0] for r in rows}


def derive(cli, instrument, granularity, interval):
    bucket = f"toStartOfInterval(ts, {interval}, toDateTime64('1970-01-01 00:00:00', 3))"
    sql = f"""
INSERT INTO oanda.candles (granularity, instrument, ts, open, high, low, close, volume)
SELECT '{granularity}', instrument,
       {bucket},
       argMin(open, ts), max(high), min(low), argMax(close, ts), sum(volume)
FROM oanda.candles_m1
WHERE instrument = '{instrument}'
GROUP BY instrument, {bucket}
"""
    cli.command(sql)


def fetch_native(token, cli, instrument, granularity):
    rows = []
    from_ts = dt.datetime.utcnow() - dt.timedelta(days=365 * 24)
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


def materialize_one(token, instrument):
    cli = ch_client()
    try:
        done = []
        for g, interval in DERIVED.items():
            if f"{instrument}|{g}" in markers(cli):
                continue
            derive(cli, instrument, g, interval)
            cli.command(
                f"INSERT INTO oanda.materialized FORMAT CSV\n{instrument},{g},0,"
                f"{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            done.append(g)
        for g in NATIVE:
            if f"{instrument}|{g}" in markers(cli):
                continue
            n = fetch_native(token, cli, instrument, g)
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
    args = ap.parse_args()

    token = get_token()
    cli = ch_client()
    pool = complete_instruments(cli)
    if args.instruments:
        pool = {i for i in args.instruments}
    instruments = sorted(pool)
    if args.axis is not None:
        instruments = [i for idx, i in enumerate(instruments) if idx % args.axes == args.axis]
    print(f"materializable={len(instruments)} axis={args.axis}/{args.axes}", flush=True)
    if not instruments:
        print("AXIS_DONE", flush=True)
        return
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = [ex.submit(materialize_one, token, inst) for inst in instruments]
        for fut in as_completed(futures):
            instrument, done, err = fut.result()
            print(f"{instrument} done={done} err={err}", flush=True)


if __name__ == "__main__":
    main()
