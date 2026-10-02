"""Offline tests for the watermark-based incremental materialisation.

No ClickHouse connection and no OANDA call is made: `ch_client`, `get_token` and
`api_get` are replaced with stubs, so this suite is safe to run anywhere.

The bug these tests guard against: `materialize_one` used to skip any
instrument|granularity already recorded in the `oanda.materialized` marker table.
Those markers were written once (2026-08-29) and never refreshed, so every derived
timeframe froze there while the workflow reported success.
"""
import datetime as dt
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "puller"))
import materialize as M


class _Result:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCLI:
    """Records every statement; answers max(ts) queries from `wm`."""

    def __init__(self, wm=None, frontier=None):
        self.wm = wm or {}
        self.frontier = frontier
        self.statements = []
        self.queries = []
        self.inserted = []

    def query(self, sql):
        self.queries.append(sql)
        if "FROM oanda.candles_m1" in sql:
            return _Result([(self.frontier,)])
        assert "max(ts)" in sql, f"unexpected query: {sql}"
        g = sql.split("granularity = '")[1].split("'")[0]
        return _Result(list(self.wm.get(g, {}).items()))

    def command(self, sql):
        self.statements.append(sql)
        return types.SimpleNamespace()

    def insert(self, table, rows, column_names=None):
        self.inserted.append((table, rows))
        return types.SimpleNamespace()

    @property
    def text(self):
        return " ".join(self.statements)


@pytest.fixture(autouse=True)
def stub_client(monkeypatch):
    """Replace every outbound dependency, and restore it after each test."""
    monkeypatch.setattr(M, "get_token", lambda: "token")
    monkeypatch.setattr(M, "fetch_native", lambda *a, **k: 7)
    yield


# --------------------------------------------------------------------------
# watermarks(): the watermark must come from the data, not the marker table
# --------------------------------------------------------------------------

def test_watermarks_reads_from_candles_not_materialized():
    cli = FakeCLI({"H1": {"EUR_USD": dt.datetime(2026, 9, 25, 20, 0)}})
    assert M.watermarks(cli, "H1") == {"EUR_USD": dt.datetime(2026, 9, 25, 20, 0)}
    assert all("materialized" not in q for q in cli.queries)


def test_watermarks_returns_empty_for_unknown_granularity():
    assert M.watermarks(FakeCLI({}), "H4") == {}


# --------------------------------------------------------------------------
# derive(): incremental vs bootstrap
# --------------------------------------------------------------------------

def test_derive_without_since_covers_full_history():
    cli = FakeCLI()
    M.derive(cli, "EUR_USD", "H1", "INTERVAL 1 HOUR")
    assert "ts >=" not in cli.text


def test_derive_with_since_adds_bound():
    cli = FakeCLI()
    M.derive(cli, "EUR_USD", "H1", "INTERVAL 1 HOUR", "2026-09-25 20:00:00")
    assert "ts >= '2026-09-25 20:00:00'" in cli.text


def test_derive_uses_final_on_m1_to_avoid_double_counted_volume():
    """candles_m1 is a ReplacingMergeTree with async dedup; without FINAL the
    boundary row can be counted twice and the inflated volume then REPLACES the
    correct row, because incremental runs always target the newest bucket."""
    cli = FakeCLI()
    M.derive(cli, "EUR_USD", "H1", "INTERVAL 1 HOUR", "2026-09-25 20:00:00")
    assert "FROM oanda.candles_m1 FINAL" in cli.text


def test_derive_pins_bucket_origin_to_utc():
    """An unpinned origin inherits the server timezone; if it ever shifts, bucket
    keys change and every bar is inserted as a duplicate instead of a replacement."""
    cli = FakeCLI()
    M.derive(cli, "EUR_USD", "D", "INTERVAL 1 DAY")
    assert "'UTC')" in cli.text
    assert "'UTC')" in cli.text.replace(" ", "").replace("3,", "3,")


# --------------------------------------------------------------------------
# _utc_naive(): the ts>= bound must be a UTC wall clock on any runner
# --------------------------------------------------------------------------

def test_utc_naive_converts_aware_to_naive_utc():
    aware = dt.datetime(2026, 9, 25, 20, 0, tzinfo=dt.timezone.utc)
    out = M._utc_naive(aware)
    assert out.tzinfo is None
    assert out.strftime("%Y-%m-%d %H:%M:%S") == "2026-09-25 20:00:00"


def test_utc_naive_shifts_non_utc_input():
    tz = dt.timezone(dt.timedelta(hours=5, minutes=30))
    aware = dt.datetime(2026, 9, 25, 20, 0, tzinfo=tz)
    assert M._utc_naive(aware).strftime("%Y-%m-%d %H:%M") == "2026-09-25 14:30"


def test_utc_naive_leaves_naive_utc_unchanged():
    naive = dt.datetime(2026, 9, 25, 20, 0)
    assert M._utc_naive(naive) is naive


# --------------------------------------------------------------------------
# materialize_one(): every derived granularity gets its own watermark
# --------------------------------------------------------------------------

def _marks(stamp):
    return {g: {"EUR_USD": stamp} for g in list(M.DERIVED) + list(M.NATIVE)}


def test_every_derived_granularity_is_rederived_incrementally(monkeypatch):
    cli = FakeCLI()
    monkeypatch.setattr(M, "ch_client", lambda: cli)
    stamp = dt.datetime(2026, 9, 25, 20, 0)
    _, _, err = M.materialize_one("token", "EUR_USD", _marks(stamp))
    assert err is None
    # one ts>= bound per DERIVED granularity: nothing may be skipped
    assert cli.text.count("ts >=") == len(M.DERIVED)


def test_no_derived_marker_write_was_dropped(monkeypatch):
    """The marker table is ops-log only, but it must still record every granularity;
    losing the derived rows is what made the table lie about derived freshness."""
    cli = FakeCLI()
    monkeypatch.setattr(M, "ch_client", lambda: cli)
    M.materialize_one("token", "EUR_USD", _marks(dt.datetime(2026, 9, 25, 20, 0)))
    logged = cli.text.count("INSERT INTO oanda.materialized")
    assert logged == len(M.DERIVED) + len(M.NATIVE)


def test_unknown_instrument_bootstraps_without_bound(monkeypatch):
    cli = FakeCLI()
    monkeypatch.setattr(M, "ch_client", lambda: cli)
    _, _, err = M.materialize_one("token", "BRANDNEW", {})
    assert err is None
    assert "ts >=" not in cli.text


def test_instrument_error_is_captured(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("clickhouse down")

    monkeypatch.setattr(M, "fetch_native", boom)
    monkeypatch.setattr(M, "ch_client", lambda: FakeCLI())
    _, done, err = M.materialize_one("token", "EUR_USD", _marks(dt.datetime(2026, 9, 25)))
    assert err is not None and "clickhouse down" in err


# --------------------------------------------------------------------------
# check_lag(): the watchdog that would have caught the original incident
# --------------------------------------------------------------------------

def test_check_lag_passes_when_within_tolerance(monkeypatch):
    frontier = dt.datetime(2026, 10, 2, 12, 0)
    marks = {"H1": {"EUR_USD": dt.datetime(2026, 10, 2, 11, 0)}}
    monkeypatch.setattr(M, "sys", types.SimpleNamespace(exit=lambda c: (_ for _ in ()).throw(SystemExit(c))))
    M.check_lag(FakeCLI(frontier=frontier), ["EUR_USD"], marks)  # no exception


def test_check_lag_fails_when_derived_behind_m1(monkeypatch):
    frontier = dt.datetime(2026, 10, 2, 12, 0)
    stale = dt.datetime(2026, 9, 25, 20, 0)  # the exact freeze we are fixing
    marks = {"H1": {"EUR_USD": stale}}
    with pytest.raises(SystemExit) as exc:
        M.check_lag(FakeCLI(frontier=frontier), ["EUR_USD"], marks)
    assert exc.value.code == 1


def test_check_lag_allows_daily_bars_to_lag_two_days(monkeypatch):
    frontier = dt.datetime(2026, 10, 2, 12, 0)
    marks = {"D": {"EUR_USD": frontier - dt.timedelta(days=1)}}
    M.check_lag(FakeCLI(frontier=frontier), ["EUR_USD"], marks)  # no exception


def test_check_lag_warns_when_candles_m1_empty(monkeypatch, capsys):
    M.check_lag(FakeCLI(frontier=None), ["EUR_USD"], {"H1": {}})
    assert "cannot verify" in capsys.readouterr().out


# --------------------------------------------------------------------------
# main(): a failed instrument must make the process exit non-zero
# --------------------------------------------------------------------------

def _run_main(monkeypatch, instruments, failing):
    monkeypatch.setattr(M, "complete_instruments", lambda cli: set(instruments))
    monkeypatch.setattr(M, "ch_client", lambda: FakeCLI())
    monkeypatch.setattr(M, "watermarks", lambda cli, g: {})
    monkeypatch.setattr(M, "check_lag", lambda *a, **k: None)
    monkeypatch.setattr(
        M, "materialize_one",
        lambda token, inst, marks=None, full=False:
            (inst, [], "ConnectionResetError(104)") if inst in failing else (inst, ["H1"], None),
    )
    monkeypatch.setattr(sys, "argv", ["materialize.py", "--workers", "2"])
    try:
        M.main()
    except SystemExit as e:
        return e.code or 0
    return 0


def test_main_exits_zero_when_all_succeed(monkeypatch):
    assert _run_main(monkeypatch, ["A", "B", "C"], failing=set()) == 0


def test_main_exits_nonzero_when_one_instrument_fails(monkeypatch):
    assert _run_main(monkeypatch, ["A", "B", "C"], failing={"B"}) == 1


def test_main_exits_nonzero_when_every_instrument_fails(monkeypatch):
    assert _run_main(monkeypatch, ["A", "B"], failing={"A", "B"}) == 1


def test_main_exits_zero_on_empty_pool(monkeypatch):
    assert _run_main(monkeypatch, [], failing=set()) == 0


# --------------------------------------------------------------------------
# api_get(): exhausting the 429 retry budget must raise, not return None
# --------------------------------------------------------------------------

def test_api_get_raises_after_rate_limit_budget(monkeypatch):
    class Resp:
        status_code = 429
        headers = {"Retry-After": "0"}

    monkeypatch.setattr(M.requests, "get", lambda *a, **k: Resp())
    monkeypatch.setattr(M.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="rate-limited"):
        M.api_get("/v3/x", "tok")