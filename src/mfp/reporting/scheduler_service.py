from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from mfp.audit.runlog import append_runlog
from mfp.config.runtime import load_config
from mfp.data.universe_sp400 import get_universe_sp400
from mfp.paper.paper_cycle import paper_reconcile
from mfp.reporting.report_jobs import run_report


def workspace_root() -> Path:
    # repo/src/mfp/reporting/scheduler_service.py -> parents[3] = repo root
    return Path(__file__).resolve().parents[3]


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _heartbeat(workspace: Path, payload: Dict[str, Any]) -> None:
    p = _state_dir(workspace) / "scheduler_heartbeat.json"
    p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8", newline="\n")


def _parse_hhmm(s: str) -> tuple[int, int]:
    hh, mm = s.strip().split(":")
    return int(hh), int(mm)


def _is_trading_day(cal, d) -> bool:
    sched = cal.schedule(start_date=d, end_date=d)
    return not sched.empty


def _next_trading_day(cal, d):
    sched = cal.schedule(start_date=d + timedelta(days=1), end_date=d + timedelta(days=14))
    if sched.empty:
        return None
    return sched.index[0].date()


def _is_last_trading_day_of_week(cal, d) -> bool:
    nxt = _next_trading_day(cal, d)
    if nxt is None:
        return False
    return (nxt.isocalendar().year, nxt.isocalendar().week) != (d.isocalendar().year, d.isocalendar().week)


def _is_last_trading_day_of_month(cal, d) -> bool:
    nxt = _next_trading_day(cal, d)
    if nxt is None:
        return False
    return (nxt.year, nxt.month) != (d.year, d.month)


def main() -> None:
    workspace = workspace_root()
    cfg = load_config(workspace)

    tz_name = cfg.get("automation", {}).get("timezone", "America/New_York")
    tz = ZoneInfo(tz_name)

    cal = mcal.get_calendar("NYSE")
    sched = BlockingScheduler(timezone=tz)

    open_h, open_m = _parse_hhmm(cfg["automation"]["market_open_time"])
    mid_h, mid_m = _parse_hhmm(cfg["automation"]["midday_time"])
    eod_h, eod_m = _parse_hhmm(cfg["automation"]["eod_time"])

    def job_open_reconcile():
        now = datetime.now(tz)
        d = now.date()
        if not _is_trading_day(cal, d):
            return

        out_dir = (
            workspace
            / "reports"
            / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_open_reconcile")
        )
        out_dir.mkdir(parents=True, exist_ok=True)

        if cfg.get("automation", {}).get("place_stops_on_reconcile", True):
            max_symbols = int(cfg.get("universe", {}).get("max_symbols", 60))
            tickers = get_universe_sp400()[:max_symbols]
            paper_reconcile(out_dir=out_dir, symbols=tickers, place_stops=True)

        append_runlog(workspace, {"kind": "scheduler", "job": "open_reconcile", "out_dir": str(out_dir)})

    def job_midday():
        now = datetime.now(tz)
        d = now.date()
        if not _is_trading_day(cal, d):
            return

        out_dir = workspace / "reports" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_midday")
        run_report(workspace=workspace, out_dir=out_dir, kind="midday")

    def job_eod():
        now = datetime.now(tz)
        d = now.date()
        if not _is_trading_day(cal, d):
            return

        out_dir = workspace / "reports" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_eod")
        run_report(workspace=workspace, out_dir=out_dir, kind="eod")

        if _is_last_trading_day_of_week(cal, d):
            out_dir2 = (
                workspace / "reports" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_eow")
            )
            run_report(workspace=workspace, out_dir=out_dir2, kind="eow")

        if _is_last_trading_day_of_month(cal, d):
            out_dir3 = (
                workspace / "reports" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_eom")
            )
            run_report(workspace=workspace, out_dir=out_dir3, kind="eom")

    def heartbeat_job():
        _heartbeat(
            workspace,
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "tz": tz_name,
                "next_jobs": [str(j.next_run_time) for j in sched.get_jobs()],
            },
        )

    # Jobs
    sched.add_job(job_open_reconcile, CronTrigger(hour=open_h, minute=open_m))
    sched.add_job(job_midday, CronTrigger(hour=mid_h, minute=mid_m))
    sched.add_job(job_eod, CronTrigger(hour=eod_h, minute=eod_m))
    sched.add_job(heartbeat_job, CronTrigger(minute="*/1"))

    print(f"[scheduler] workspace={workspace}")
    print(f"[scheduler] timezone={tz_name}")
    print("[scheduler] jobs:")
    for j in sched.get_jobs():
        print(" -", j)

    sched.start()


if __name__ == "__main__":
    main()
