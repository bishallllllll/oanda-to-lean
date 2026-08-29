#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import time

import requests

API_HOST = "https://api-fxpractice.oanda.com"
GRAN = "M1"
PAGE = 5000
STEP = dt.timedelta(minutes=1)
TOKEN_FILE = os.environ.get(
    "OANDA_TOKEN_FILE",
    os.path.expanduser("~/.openalice/provider-keys.json"),
)
CH_ACCOUNT = os.environ.get("COMPOSIO_CH_ACCOUNT", "clickhouse_shuck-dietal")
CH_DB = "oanda"
CH_TABLE = "candles_m1"


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
            wait = float(r.headers.get("Retry-After", 1)) * (attempt + 1)
            time.sleep(min(wait, 30))
            continue
        r.raise_for_status()
        return r.json()


def list_instruments(token, account_id):
    data = api_get(f"/v3/accounts/{account_id}/instruments", token)
    return [i["name"] for i in data["instruments"]]


def first_account(token):
    data = api_get("/v3/accounts", token)
    return data["accounts"][0]["id"]


def query_page(token, instrument, from_ts):
    params = {
        "granularity": GRAN,
        "price": "M",
        "count": PAGE,
        "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "includeIncomplete": "false",
    }
    data = api_get(f"/v3/instruments/{instrument}/candles", token, params)
    rows = []
    last = from_ts
    for c in data["candles"]:
        rows.append(
            (c["time"], c["mid"]["o"], c["mid"]["h"], c["mid"]["l"], c["mid"]["c"], c["volume"])
        )
        last = dt.datetime.fromisoformat(c["time"].replace("Z", "+00:00")).replace(tzinfo=None)
    return rows, last + STEP


def ch_insert_csv(rows_csv_path, mode):
    query = (
        f"INSERT INTO {CH_DB}.{CH_TABLE} "
        "(instrument, ts, open, high, low, close, volume) FORMAT CSV"
    )
    payload = {"query": query + "\n" + open(rows_csv_path).read(),
               "settings": {"max_query_size": 20000000, "input_format_allow_errors_ratio": 0.01}}
    if mode == "composio":
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        subprocess.run(
            ["composio", "execute", "CLICKHOUSE_EXECUTE_QUERY", "-d", f"@{path}",
             "--account", CH_ACCOUNT],
            check=True, capture_output=True, text=True, timeout=300,
        )
        os.unlink(path)
    else:
        import clickhouse_connect

        cli = clickhouse_connect.get_client(
            host=os.environ["CH_HOST"],
            username=os.environ["CH_USER"],
            password=os.environ["CH_PASSWORD"],
            port=int(os.environ.get("CH_PORT", "8443")),
            secure=True,
        )
        cli.raw_query(payload["query"])


def upsert_status(mode, instrument, last_ts):
    ts = last_ts.strftime("%Y-%m-%d %H:%M:%S")
    query = (
        f"INSERT INTO {CH_DB}.ingest_status FORMAT CSV\n"
        f"{instrument},{GRAN},{ts},{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    payload = {"query": query}
    if mode == "composio":
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        subprocess.run(
            ["composio", "execute", "CLICKHOUSE_EXECUTE_QUERY", "-d", f"@{path}",
             "--account", CH_ACCOUNT],
            check=True, capture_output=True, text=True, timeout=120,
        )
        os.unlink(path)
    else:
        import clickhouse_connect

        cli = clickhouse_connect.get_client(
            host=os.environ["CH_HOST"],
            username=os.environ["CH_USER"],
            password=os.environ["CH_PASSWORD"],
            port=int(os.environ.get("CH_PORT", "8443")),
            secure=True,
        )
        cli.raw_query(payload["query"])


def read_state(state_dir, instrument):
    p = os.path.join(state_dir, f"{instrument}.json")
    if os.path.exists(p):
        try:
            with open(p) as f:
                return dt.datetime.fromisoformat(json.load(f)["until"])
        except (ValueError, KeyError, json.JSONDecodeError):
            return None
    return None


def write_state(state_dir, instrument, until):
    os.makedirs(state_dir, exist_ok=True)
    p = os.path.join(state_dir, f"{instrument}.json")
    with open(p, "w") as f:
        json.dump({"until": until.isoformat()}, f)


def read_ch_checkpoint(mode):
    if mode != "direct":
        return {}
    query = "SELECT instrument, toString(max(last_ts)) FROM oanda.ingest_status GROUP BY instrument"
    if mode == "direct":
        import clickhouse_connect

        cli = clickhouse_connect.get_client(
            host=os.environ["CH_HOST"],
            username=os.environ["CH_USER"],
            password=os.environ["CH_PASSWORD"],
            port=int(os.environ.get("CH_PORT", "8443")),
            secure=True,
        )
        rows = cli.query(query).result_rows
        return {r[0]: dt.datetime.fromisoformat(r[1]) for r in rows}
    return {}


def run_instrument(token, instrument, state_dir, mode, depth, since, budget, started):
    now = dt.datetime.utcnow()
    start = since or (now - dt.timedelta(days=int(depth) * 365))
    from_ts = ch_state.get(instrument) or read_state(state_dir, instrument)
    if from_ts is None or from_ts < start:
        from_ts = start
    changed = False
    while from_ts < now:
        if budget and (dt.datetime.utcnow() - started).total_seconds() > budget * 60:
            print(f"{instrument} budget-reached at {from_ts}", flush=True)
            break
        rows, nxt = query_page(token, instrument, from_ts)
        if rows:
            with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
                for (t, o, h, l, c, v) in rows:
                    f.write(f"{instrument},{t[0:19].replace('T', ' ')}.{t[20:23]},{o},{h},{l},{c},{v}\n")
                path = f.name
            try:
                ch_insert_csv(path, mode)
            finally:
                os.unlink(path)
            chips = len(rows)
            write_state(state_dir, instrument, nxt - STEP)
            upsert_status(mode, instrument, nxt - STEP)
            changed = True
            if chips < PAGE:
                break
        from_ts = nxt
        time.sleep(0.45)
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--depth-years", type=int, default=5)
    ap.add_argument("--instruments", nargs="*", default=None)
    ap.add_argument("--mode", choices=["composio", "direct"], default="composio")
    ap.add_argument("--state-dir", default="/tmp/oanda-puller-state")
    ap.add_argument("--max-minutes", type=int, default=None)
    ap.add_argument("--since", default=None)
    args = ap.parse_args()

    global ch_state
    ch_state = read_ch_checkpoint(args.mode)
    since = dt.datetime.fromisoformat(args.since.replace("Z", "+00:00")).replace(tzinfo=None) if args.since else None
    started = dt.datetime.utcnow()

    token = get_token()
    account = first_account(token)
    instruments = args.instruments or list_instruments(token, account)
    print(f"account={account} instruments={len(instruments)} mode={args.mode} budget={args.max_minutes}min", flush=True)
    for inst in instruments:
        ok = run_instrument(token, inst, args.state_dir, args.mode, args.depth_years,
                            since, args.max_minutes, started)
        print(f"{inst} done={ok}", flush=True)
        if args.max_minutes and (dt.datetime.utcnow() - started).total_seconds() > args.max_minutes * 60:
            print("run budget exhausted, exiting", flush=True)
            sys.exit(0)


if __name__ == "__main__":
    main()
