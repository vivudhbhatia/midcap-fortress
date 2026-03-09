from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from mfp.audit.integrity import verify_manifest
from mfp.config.runtime import config_hash, load_config
from mfp.governance.certificates import load_pretrade_certificate, validate_pretrade_certificate
from mfp.governance.guardrails import check_guardrails
from mfp.governance.proposals import (
    apply_proposal,
    approve_proposal,
    create_proposal_from_dials,
    list_proposals,
    load_proposal,
    read_changelog,
    reject_proposal,
)
from mfp.ui.explain_trade import explain_order, load_run_context
from mfp.ui.github_opsbot import run_command


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def open_folder(p: Path) -> None:
    try:
        os.startfile(str(p))  # Windows
    except Exception:
        pass


def _short(h: str) -> str:
    return h[:10]


def list_run_dirs(workspace: Path, limit: int = 120) -> list[Path]:
    reports = workspace / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)
    return runs[:limit]


def find_latest_run_with(workspace: Path, filename: str) -> Optional[Path]:
    for r in list_run_dirs(workspace, limit=300):
        if (r / filename).exists():
            return r
    return None


def read_json_safe(p: Path) -> Optional[Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_csv_safe(p: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def orders_table(orders: list[dict]) -> pd.DataFrame:
    rows = []
    for o in orders:
        rows.append(
            {
                "side": o.get("side"),
                "symbol": o.get("symbol"),
                "qty": o.get("qty"),
                "reason": o.get("reason"),
                "close": o.get("close"),
                "rsi": o.get("rsi2"),
                "stop_est": o.get("stop_estimate"),
                "risk_usd": o.get("risk_usd"),
            }
        )
    return pd.DataFrame(rows)


st.set_page_config(page_title="Midcap Fortress — Control Center", layout="wide")

workspace = workspace_root()
cfg_current = load_config(workspace)
settings_id = config_hash(cfg_current)

# --- Simple vs Advanced toggle (defaults to Simple) ---
with st.sidebar:
    st.header("View mode")
    advanced = st.toggle(
        "Advanced mode", value=False, help="Shows raw files, JSON details, and more controls."
    )
    st.markdown("---")
    st.write("**Safety Check** = Backtest sweep + human acknowledgement.")
    st.write("**Settings ID** = fingerprint of your settings. If it changes, re-run Safety Check.")

st.title("Midcap Fortress — Control Center")
st.caption("Built for transparency. Every action creates files you can open, review, and verify.")

tabs = st.tabs(
    [
        "Daily Flow (1 screen)",
        "Backtests",
        "Practice Trading",
        "Activity & Files",
        "Change Settings (with approvals)",
        "Prototype",
    ]
)

# ======================================================================================
# 1) Daily flow (one screen)
# ======================================================================================
with tabs[0]:
    st.subheader("Daily flow")
    st.caption("Recommended: Safety Check → Preview trades → Place paper orders → Review results")

    # Live gate status
    val = validate_pretrade_certificate(workspace, cfg_current)
    cert = load_pretrade_certificate(workspace)

    # Step 1: Safety Check
    st.markdown("## Step 1 — Run Safety Check (backtest sweep)")
    col1, col2 = st.columns([1.2, 1.8])
    with col1:
        if st.button("Run Safety Check now"):
            start = cfg_current.get("pretrade_check", {}).get("start", "2011-01-01")
            end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            max_symbols = int(cfg_current.get("pretrade_check", {}).get("max_symbols", 60))
            res = run_command(
                f"/mfp backtest-sweep start={start} end={end} max_symbols={max_symbols}", workspace=workspace
            )
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.session_state["last_safety_dir"] = str(res.artifacts_dir)
                st.button("Open Safety Check files", on_click=open_folder, args=(res.artifacts_dir,))
            st.rerun()
    with col2:
        if val["ok"]:
            st.success("Safety Check status: READY ✅")
        else:
            st.warning("Safety Check status: NOT READY ⚠️")
        st.write("Reason:", val["reason"])
        st.write("Current Settings ID:", f"`{_short(settings_id)}`")
        if cert:
            st.caption("Latest Safety Check record:")
            st.json(
                {
                    k: cert.get(k)
                    for k in ["created_ts_utc", "pass", "reviewed", "config_hash", "sweep_out_dir"]
                }
            )

    # Step 2: Acknowledge
    st.markdown("## Step 2 — Confirm you reviewed the Safety Check results")
    if st.button("I reviewed the Safety Check results (acknowledge)"):
        res = run_command("/mfp pretrade-ack", workspace=workspace)
        st.markdown(res.summary_md)
        st.rerun()

    st.markdown("---")

    # Step 3: Preview trades
    st.markdown("## Step 3 — Preview trades (no orders)")
    if st.button("Preview trades now"):
        ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
        res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=true", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_preview_dir"] = str(res.artifacts_dir)
            st.button("Open preview files", on_click=open_folder, args=(res.artifacts_dir,))
        st.rerun()

    # Show preview results (latest or last session)
    preview_dir = None
    if "last_preview_dir" in st.session_state:
        p = Path(st.session_state["last_preview_dir"])
        if p.exists():
            preview_dir = p
    if preview_dir is None:
        preview_dir = find_latest_run_with(workspace, "orderIntents.json")

    if preview_dir:
        st.markdown(f"### Latest preview: `{preview_dir.name}`")
        intents = read_json_safe(preview_dir / "orderIntents.json") or []
        if intents:
            df = orders_table(intents)
            st.dataframe(df, use_container_width=True)

            st.markdown("### Why was this trade suggested?")
            choice = st.selectbox(
                "Choose a trade to explain",
                [
                    f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                    for o in intents
                ],
            )
            idx = [
                f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                for o in intents
            ].index(choice)
            order = intents[idx]

            ctx = load_run_context(preview_dir, workspace)
            exp = explain_order(order, ctx)

            st.info(exp["summary"])
            st.markdown("**Signal checks (why it triggered):**")
            for k, v in exp["signal_checks"].items():
                st.write(("✅" if v else "❌") + " " + k)

            st.markdown("**Filters (basic safety filters):**")
            for k, v in exp["filter_checks"].items():
                st.write(("✅" if v else "❌") + " " + k)

            st.markdown("**Sizing (how big and why):**")
            st.write(exp["sizing"])

            st.markdown("**Safety checks (must pass before orders can be placed):**")
            st.write(exp["safety"])

            if advanced:
                st.markdown("**Raw order (advanced):**")
                st.json(exp["raw_order"])
        else:
            st.caption("No orders suggested in this preview.")
    else:
        st.caption("No preview found yet. Click “Preview trades now” to generate one.")

    st.markdown("---")

    # Step 4: Place paper orders
    st.markdown("## Step 4 — Place paper orders (paper trading)")
    if not val["ok"]:
        st.warning("Blocked until Safety Check is ready + acknowledged.")
        st.caption(val["reason"])

    if st.button("Place paper orders", disabled=not val["ok"]):
        ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
        res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=false", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_exec_dir"] = str(res.artifacts_dir)
            st.button("Open execution files", on_click=open_folder, args=(res.artifacts_dir,))
        st.rerun()

    st.markdown("---")

    # Step 5: View results
    st.markdown("## Step 5 — View results")
    exec_dir = None
    if "last_exec_dir" in st.session_state:
        p = Path(st.session_state["last_exec_dir"])
        if p.exists():
            exec_dir = p

    if exec_dir:
        st.success(f"Latest execution: `{exec_dir.name}`")
        ack = (
            read_json_safe(exec_dir / "orders.json")
            or read_json_safe(exec_dir / "paper_broker_ack.json")
            or {}
        )
        st.write("Placed:", len(ack.get("placed", [])))
        st.write("Skipped:", len(ack.get("skipped", [])))
        st.button("Open execution folder", on_click=open_folder, args=(exec_dir,))
        if advanced:
            st.json(ack)
    else:
        st.caption("No execution run yet today (that’s okay).")

# ======================================================================================
# 2) Backtests
# ======================================================================================
with tabs[1]:
    st.subheader("Backtests")
    st.caption("Run backtests to evaluate results across time horizons before execution.")

    start = st.text_input(
        "Start date (YYYY-MM-DD)", cfg_current.get("pretrade_check", {}).get("start", "2011-01-01")
    )
    end = st.text_input("End date (blank = today)", cfg_current.get("pretrade_check", {}).get("end", ""))
    max_symbols = st.number_input(
        "Universe size (max symbols)",
        10,
        400,
        int(cfg_current.get("pretrade_check", {}).get("max_symbols", 60)),
        1,
    )

    if st.button("Run backtest sweep (1d / 1wk / 1mo)"):
        end2 = end.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cmd = f"/mfp backtest-sweep start={start} end={end2} max_symbols={int(max_symbols)}"
        res = run_command(cmd, workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            sweep_path = res.artifacts_dir / "sweep.json"
            if sweep_path.exists():
                sweep = json.loads(sweep_path.read_text(encoding="utf-8"))
                st.dataframe(pd.DataFrame(sweep.get("rows", [])))
            st.button("Open sweep folder", on_click=open_folder, args=(res.artifacts_dir,))

# ======================================================================================
# 3) Practice Trading
# ======================================================================================
with tabs[2]:
    st.subheader("Practice Trading (paper)")
    st.caption("Preview trades anytime. Placing paper orders is blocked until Safety Check is ready.")

    ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
    val = validate_pretrade_certificate(workspace, cfg_current)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Preview trades (no orders)"):
            res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=true", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open files", on_click=open_folder, args=(res.artifacts_dir,))
    with col2:
        if st.button("Place paper orders", disabled=not val["ok"]):
            res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=false", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open files", on_click=open_folder, args=(res.artifacts_dir,))

    if not val["ok"]:
        st.warning("Blocked until Safety Check passes + acknowledged.")
        st.caption(val["reason"])

# ======================================================================================
# 4) Activity & Files (includes “Why?” panel for any run)
# ======================================================================================
with tabs[3]:
    st.subheader("Activity & Files")
    st.caption("Browse any run, open artifacts, and run a tamper check (integrity).")

    runs = list_run_dirs(workspace, limit=120)
    if not runs:
        st.info("No runs found yet.")
    else:
        run_name = st.selectbox("Choose a run folder", [r.name for r in runs])
        run_dir = workspace / "reports" / run_name

        if st.button("Open folder in Explorer"):
            open_folder(run_dir)

        integ = verify_manifest(run_dir)
        if integ.get("ok"):
            st.success("Integrity check: PASS ✅")
        else:
            st.error("Integrity check: FAIL ❌")
            if advanced:
                st.json(integ)

        intents = read_json_safe(run_dir / "orderIntents.json") or []
        if intents:
            st.markdown("### Orders in this run")
            st.dataframe(orders_table(intents), use_container_width=True)

            st.markdown("### Why was this trade suggested?")
            pick = st.selectbox(
                "Pick an order to explain",
                [
                    f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                    for o in intents
                ],
                key="why_pick_ledger",
            )
            idx = [
                f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                for o in intents
            ].index(pick)
            order = intents[idx]
            ctx = load_run_context(run_dir, workspace)
            exp = explain_order(order, ctx)

            st.info(exp["summary"])
            st.write("Signal checks:", exp["signal_checks"])
            st.write("Filters:", exp["filter_checks"])
            st.write("Sizing:", exp["sizing"])
            st.write("Safety:", exp["safety"])

            if advanced:
                st.json(exp)
        else:
            st.caption("No order intents found in this run.")

        if advanced:
            st.markdown("---")
            st.markdown("### File viewer (advanced)")
            files = sorted([p for p in run_dir.glob("*") if p.is_file()], key=lambda x: x.name)
            fsel = st.selectbox("Open a file", [p.name for p in files])
            fp = run_dir / fsel
            if fp.suffix.lower() == ".json":
                st.json(read_json_safe(fp))
            elif fp.suffix.lower() in (".md", ".txt"):
                st.markdown(fp.read_text(encoding="utf-8", errors="ignore"))
            elif fp.suffix.lower() == ".csv":
                df = read_csv_safe(fp)
                if df is None:
                    st.error("Could not read CSV.")
                else:
                    st.dataframe(df)
            else:
                st.code(fp.read_text(encoding="utf-8", errors="ignore")[:5000])

# ======================================================================================
# 5) Change Settings (with approvals)
# ======================================================================================
with tabs[4]:
    st.subheader("Change Settings (with approvals)")
    st.caption("Changes flow: Create → Approve → Apply. Unsafe changes are blocked.")

    st.markdown(f"**Current Settings ID:** `{_short(settings_id)}`")

    draft = json.loads(json.dumps(cfg_current))  # safe deep copy

    c1, c2, c3 = st.columns(3)
    with c1:
        draft["universe"]["max_symbols"] = st.number_input(
            "Universe size (max symbols)", 10, 400, int(draft["universe"]["max_symbols"]), 1
        )
        draft["proxy"]["ticker"] = st.text_input(
            "Market proxy ticker (IJH/MDY)", str(draft["proxy"]["ticker"])
        )
        draft["signal"]["rsi_buy_below"] = st.number_input(
            "Buy when RSI is below", 1.0, 40.0, float(draft["signal"]["rsi_buy_below"]), 1.0
        )

    with c2:
        draft["risk"]["max_positions"] = st.number_input(
            "Max positions", 1, 20, int(draft["risk"]["max_positions"]), 1
        )
        draft["risk"]["risk_per_trade_pct"] = st.number_input(
            "Risk per trade (%)", 0.01, 2.0, float(draft["risk"]["risk_per_trade_pct"]), 0.01
        )
        draft["risk"]["max_open_risk_pct"] = st.number_input(
            "Total open risk cap (%)", 0.10, 5.0, float(draft["risk"]["max_open_risk_pct"]), 0.10
        )

    with c3:
        draft["risk"]["stop_atr_mult"] = st.number_input(
            "Stop distance (ATR multiple)", 0.5, 5.0, float(draft["risk"]["stop_atr_mult"]), 0.1
        )
        draft["execution"]["pretrade_check_max_age_days"] = st.number_input(
            "Safety Check expires (days)", 1, 30, int(draft["execution"]["pretrade_check_max_age_days"]), 1
        )
        draft["risk"]["scale_in_mode"] = st.selectbox(
            "Scale-in behavior",
            ["none", "confirm_add"],
            index=["none", "confirm_add"].index(str(draft["risk"]["scale_in_mode"]))
            if str(draft["risk"]["scale_in_mode"]) in ["none", "confirm_add"]
            else 0,
        )

    draft_id = config_hash(draft)
    st.info(f"Draft Settings ID: `{_short(draft_id)}`")

    guard = check_guardrails(draft)
    if guard["ok"]:
        st.success("Safety rules: PASS ✅")
    else:
        st.error("Safety rules: FAIL ❌ (will be blocked from Apply)")
        for v in guard["violations"]:
            st.write(f"- ❌ {v['message']}")
    if guard["warnings"]:
        st.warning("Warnings (allowed, but consider reviewing):")
        for w in guard["warnings"]:
            st.write(f"- ⚠️ {w['message']}")

    if st.button("Create Change Request"):
        try:
            pr = create_proposal_from_dials(workspace, proposed_cfg=draft, created_by="human")
            st.success(f"Created Change Request: {pr['proposal_id']}")
            st.rerun()
        except Exception as e:
            st.error(f"Could not create change request: {type(e).__name__}: {e}")

    proposals = list_proposals(workspace)
    st.markdown("### Change requests")
    if not proposals:
        st.caption("No change requests yet.")
    else:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "proposal_id": p["proposal_id"],
                        "status": p["status"],
                        "from_settings_id": _short(p["base_config_hash"]),
                        "to_settings_id": _short(p["proposed_config_hash"]),
                        "safety_ok": bool(p.get("guardrails", {}).get("ok", False)),
                        "created": p.get("created_ts_utc"),
                    }
                    for p in proposals
                ]
            )
        )

        sel = st.selectbox("Select a change request", [p["proposal_id"] for p in proposals])
        pr = load_proposal(workspace, sel)

        st.markdown("#### What will change")
        st.dataframe(pd.DataFrame(pr.get("changes", [])))

        colA, colB, colC = st.columns(3)
        with colA:
            if st.button("Approve"):
                try:
                    approve_proposal(workspace, sel, approved_by="human")
                    st.success("Approved.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Approve failed: {type(e).__name__}: {e}")

        with colB:
            can_apply = (pr["status"] == "APPROVED") and bool(pr.get("guardrails", {}).get("ok", False))
            if st.button("Apply (make active)", disabled=not can_apply):
                try:
                    apply_proposal(workspace, sel, applied_by="human")
                    st.success("Applied. Your Settings ID changed — run Safety Check again.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Apply failed: {type(e).__name__}: {e}")

        with colC:
            if st.button("Reject"):
                try:
                    reject_proposal(workspace, sel, rejected_by="human", reason="Rejected in UI")
                    st.success("Rejected.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Reject failed: {type(e).__name__}: {e}")

    st.markdown("---")
    st.markdown("### Change history (Settings ID lineage)")
    hist = read_changelog(workspace, limit=200)
    if hist:
        st.dataframe(pd.DataFrame(hist))
    else:
        st.caption("No applied changes yet.")

# ======================================================================================
# 6) Prototype
# ======================================================================================
with tabs[5]:
    st.subheader("Prototype (embedded HTML)")
    st.caption("Put your file here: prototype/fortress_trading_agent_prototype.html")

    proto = workspace / "prototype" / "fortress_trading_agent_prototype.html"
    if proto.exists():
        html = proto.read_text(encoding="utf-8", errors="ignore")
        components.html(html, height=900, scrolling=True)
        st.button("Open prototype folder", on_click=open_folder, args=(proto.parent,))
    else:
        st.info("Prototype file not found yet.")
