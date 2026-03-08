from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import streamlit as st

from mfp.audit.runlog import read_runlog
from mfp.config.runtime import config_hash, load_config, save_config, snapshot_config
from mfp.ui.github_opsbot import run_command


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def open_folder(p: Path) -> None:
    try:
        os.startfile(str(p))  # Windows
    except Exception:
        pass


st.set_page_config(page_title="Midcap Fortress Control Center", layout="wide")

workspace = workspace_root()
cfg = load_config(workspace)

st.title("Midcap Fortress Control Center")
st.caption(
    "Transparency + auditability by default. Every run produces evidence.zip + manifest + decision_trace."
)

tab_dials, tab_run, tab_reports, tab_evidence, tab_scheduler = st.tabs(
    ["Dials", "Run Now", "Reports", "Evidence", "Scheduler"]
)

with tab_dials:
    st.subheader("Strategy dials (saved to state/config.json)")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### Universe / Proxy")
        cfg["universe"]["max_symbols"] = st.number_input(
            "Max symbols", 10, 400, int(cfg["universe"]["max_symbols"]), 1
        )
        cfg["proxy"]["ticker"] = st.text_input("Regime proxy ticker", cfg["proxy"]["ticker"])
        cfg["proxy"]["monthly_sma_months"] = st.number_input(
            "Monthly SMA months", 3, 24, int(cfg["proxy"]["monthly_sma_months"]), 1
        )
        cfg["proxy"]["daily_sma"] = st.number_input(
            "Fast risk SMA (days)", 50, 300, int(cfg["proxy"]["daily_sma"]), 10
        )
        cfg["proxy"]["shock_2d_drop_pct"] = st.number_input(
            "2-day shock drop (e.g. 0.04 = 4%)", 0.01, 0.20, float(cfg["proxy"]["shock_2d_drop_pct"]), 0.01
        )
        cfg["proxy"]["shock_pause_days"] = st.number_input(
            "Shock pause days", 1, 10, int(cfg["proxy"]["shock_pause_days"]), 1
        )

    with col2:
        st.markdown("### Signals")
        cfg["signal"]["trend_sma"] = st.number_input(
            "Trend SMA (days)", 50, 300, int(cfg["signal"]["trend_sma"]), 10
        )
        cfg["signal"]["fast_sma"] = st.number_input(
            "Fast SMA (days)", 2, 30, int(cfg["signal"]["fast_sma"]), 1
        )
        cfg["signal"]["rsi_len"] = st.number_input("RSI length", 2, 14, int(cfg["signal"]["rsi_len"]), 1)
        cfg["signal"]["rsi_buy_below"] = st.number_input(
            "RSI buy below", 1.0, 40.0, float(cfg["signal"]["rsi_buy_below"]), 1.0
        )
        cfg["signal"]["rsi_exit_above"] = st.number_input(
            "RSI exit above", 50.0, 95.0, float(cfg["signal"]["rsi_exit_above"]), 1.0
        )
        cfg["signal"]["atr_len"] = st.number_input("ATR length", 5, 30, int(cfg["signal"]["atr_len"]), 1)
        cfg["signal"]["time_stop_bars"] = st.number_input(
            "Time stop (bars)", 2, 30, int(cfg["signal"]["time_stop_bars"]), 1
        )

    with col3:
        st.markdown("### Risk / Filters")
        cfg["risk"]["max_positions"] = st.number_input(
            "Max positions", 1, 20, int(cfg["risk"]["max_positions"]), 1
        )
        cfg["risk"]["risk_per_trade_pct"] = st.number_input(
            "Risk per trade (%)", 0.01, 2.0, float(cfg["risk"]["risk_per_trade_pct"]), 0.01
        )
        cfg["risk"]["max_open_risk_pct"] = st.number_input(
            "Max open risk (%)", 0.10, 5.0, float(cfg["risk"]["max_open_risk_pct"]), 0.10
        )
        cfg["risk"]["stop_atr_mult"] = st.number_input(
            "Stop ATR multiplier", 0.5, 5.0, float(cfg["risk"]["stop_atr_mult"]), 0.1
        )
        cfg["filters"]["min_price"] = st.number_input(
            "Min price", 1.0, 200.0, float(cfg["filters"]["min_price"]), 1.0
        )
        cfg["filters"]["min_adv20"] = st.number_input(
            "Min ADV20 ($)", 1_000_000.0, 200_000_000.0, float(cfg["filters"]["min_adv20"]), 1_000_000.0
        )
        cfg["filters"]["max_atr_pct"] = st.number_input(
            "Max ATR% (e.g. 0.08)", 0.01, 0.30, float(cfg["filters"]["max_atr_pct"]), 0.01
        )

    st.markdown("---")
    ch = config_hash(cfg)
    st.info(f"Config hash: `{ch}`")

    if st.button("Save dials"):
        save_config(workspace, cfg)
        snap = snapshot_config(workspace, cfg)
        st.success(f"Saved. Snapshot: {snap}")
        st.rerun()

with tab_run:
    st.subheader("Run commands (no terminal)")

    colA, colB = st.columns(2)
    with colA:
        if st.button("Status Now (paper-status)"):
            res = run_command("/mfp paper-status", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                if st.button("Open artifacts folder"):
                    open_folder(res.artifacts_dir)

        st.markdown("### Pre-trade backtest (required before execution)")
        st.caption("Runs 1d/1wk/1mo sweep. You should review results before placing trades.")
        if st.button("Run Pre-trade Backtest Sweep"):
            # Use existing sweep command (fast path). You can adjust via CLI later.
            start = cfg["pretrade_check"]["start"]
            end = cfg["pretrade_check"]["end"] or datetime.utcnow().strftime("%Y-%m-%d")
            max_symbols = int(cfg["pretrade_check"]["max_symbols"])
            cmd = f"/mfp backtest-sweep start={start} end={end} max_symbols={max_symbols}"
            res = run_command(cmd, workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.success("Backtest sweep complete. Review reports before executing trades.")
                st.button("Open sweep artifacts", on_click=open_folder, args=(res.artifacts_dir,))

    with colB:
        st.markdown("### Paper cycle")
        max_symbols = int(cfg["universe"]["max_symbols"])

        if st.button("Plan Trades (dry-run)"):
            res = run_command(f"/mfp paper-cycle max_symbols={max_symbols} dry_run=true", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open plan artifacts", on_click=open_folder, args=(res.artifacts_dir,))

        st.warning(
            "Execution requires you to have reviewed backtests. Keep MFP_TRADING_ENABLED=false until ready."
        )
        if st.button("Execute Paper Trades (place orders)"):
            res = run_command(
                f"/mfp paper-cycle max_symbols={max_symbols} dry_run=false", workspace=workspace
            )
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open execution artifacts", on_click=open_folder, args=(res.artifacts_dir,))

        if st.button("Reconcile / Place Stops"):
            res = run_command(
                f"/mfp paper-reconcile max_symbols={max_symbols} place_stops=true", workspace=workspace
            )
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open reconcile artifacts", on_click=open_folder, args=(res.artifacts_dir,))

with tab_reports:
    st.subheader("Automated reports (midday / EOD / EOW / EOM)")
    st.caption("Generated by scheduler service into reports/<run_id>_* folders.")

    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports_dir.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)[:30]

    for p in runs[:15]:
        st.write(p.name)
        md_files = list(p.glob("*_report.md"))
        if md_files:
            md = md_files[0].read_text(encoding="utf-8", errors="ignore")
            with st.expander(f"View {md_files[0].name}"):
                st.markdown(md)
        if st.button(f"Open {p.name}", key=f"open_{p.name}"):
            open_folder(p)

with tab_evidence:
    st.subheader("Run log (audit trail)")
    items = read_runlog(workspace, limit=100)
    if not items:
        st.info("No runlog yet. Run something to generate evidence.")
    for it in items[:25]:
        st.write(it)

with tab_scheduler:
    st.subheader("Scheduler")
    st.markdown(
        "Run the scheduler (auto midday/EOD/week/month) by double-clicking `Start_Scheduler.bat`.\n\n"
        "Scheduler heartbeat lives in `state/scheduler_heartbeat.json`."
    )
    hb = workspace / "state" / "scheduler_heartbeat.json"
    if hb.exists():
        st.json(json.loads(hb.read_text(encoding="utf-8")))
    else:
        st.info("No heartbeat yet. Start the scheduler.")
