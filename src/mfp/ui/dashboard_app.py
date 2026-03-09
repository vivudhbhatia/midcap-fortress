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


def _load_json(p: Path) -> Optional[Any]:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def list_run_dirs(workspace: Path, limit: int = 200) -> list[Path]:
    reports = workspace / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)
    return runs[:limit]


def find_latest_run_with(workspace: Path, filename: str) -> Optional[Path]:
    for r in list_run_dirs(workspace, limit=400):
        if (r / filename).exists():
            return r
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
                "rsi2": o.get("rsi2"),
                "stop_est": o.get("stop_estimate"),
                "risk_usd": o.get("risk_usd"),
            }
        )
    return pd.DataFrame(rows)


st.set_page_config(page_title="Midcap Fortress — Control Center", layout="wide")

workspace = workspace_root()
cfg_current = load_config(workspace)
settings_id = config_hash(cfg_current)

# ---------------- Sidebar: Simple vs Advanced ----------------
with st.sidebar:
    st.header("View mode")
    advanced = st.toggle("Advanced mode", value=False, key="mode_advanced")
    st.markdown("---")
    st.write("**Safety Check** = backtest sweep + your acknowledgement.")
    st.write("**Settings ID** = fingerprint of your settings. If it changes, re-run Safety Check.")
    st.write("**Preview trades** = see suggested trades (no orders).")

st.title("Midcap Fortress — Control Center")
st.caption("Transparency-first: every action writes evidence files you can open and verify.")

# Tabs (Simple default: hides Practice Trading tab)
if not advanced:
    tab_names = ["Daily Flow (1 screen)", "Backtests", "Activity & Files", "Change Settings", "Prototype"]
else:
    tab_names = [
        "Daily Flow (1 screen)",
        "Backtests",
        "Practice Trading",
        "Activity & Files",
        "Change Settings",
        "Prototype",
    ]

tabs = st.tabs(tab_names)

# ======================================================================================
# TAB 0: Daily Flow (one screen)
# ======================================================================================
with tabs[0]:
    st.subheader("Daily Flow")
    st.caption("Safety Check → Acknowledge → Preview → Place → Review results")

    val = validate_pretrade_certificate(workspace, cfg_current)
    cert = load_pretrade_certificate(workspace)

    st.markdown("## Step 1 — Safety Check (backtest sweep)")
    c1, c2, c3 = st.columns([1.2, 1.2, 1.6])

    with c1:
        if st.button("Run Safety Check now", key="daily_run_safety"):
            start = cfg_current.get("pretrade_check", {}).get("start", "2011-01-01")
            end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            max_symbols = int(cfg_current.get("pretrade_check", {}).get("max_symbols", 60))
            res = run_command(
                f"/mfp backtest-sweep start={start} end={end} max_symbols={max_symbols}",
                workspace=workspace,
            )
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.session_state["last_sweep_dir"] = str(res.artifacts_dir)
            st.rerun()

    with c2:
        if st.button("I reviewed the results (acknowledge)", key="daily_ack_safety"):
            res = run_command("/mfp pretrade-ack", workspace=workspace)
            st.markdown(res.summary_md)
            st.rerun()

    with c3:
        if val["ok"]:
            st.success("Safety Check: READY ✅")
        else:
            st.warning("Safety Check: NOT READY ⚠️")
        st.write("Reason:", val["reason"])
        st.write("Current Settings ID:", f"`{_short(settings_id)}`")
        if cert:
            st.caption("Latest Safety Check record")
            st.json(
                {
                    k: cert.get(k)
                    for k in ["created_ts_utc", "pass", "reviewed", "config_hash", "sweep_out_dir"]
                }
            )

    sweep_dir = None
    if "last_sweep_dir" in st.session_state:
        p = Path(st.session_state["last_sweep_dir"])
        if p.exists():
            sweep_dir = p
    if sweep_dir is None:
        sweep_dir = find_latest_run_with(workspace, "sweep.json")

    if sweep_dir:
        sweep = _load_json(sweep_dir / "sweep.json")
        if isinstance(sweep, dict) and isinstance(sweep.get("rows"), list):
            st.markdown("### Latest sweep summary")
            st.dataframe(pd.DataFrame(sweep["rows"]), use_container_width=True)
        if st.button("Open sweep folder", key="daily_open_sweep"):
            open_folder(sweep_dir)

    st.markdown("---")

    st.markdown("## Step 2 — Preview trades (no orders)")
    if st.button("Preview trades now", key="daily_preview_trades"):
        ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
        res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=true", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_preview_dir"] = str(res.artifacts_dir)
        st.rerun()

    preview_dir = None
    if "last_preview_dir" in st.session_state:
        p = Path(st.session_state["last_preview_dir"])
        if p.exists():
            preview_dir = p
    if preview_dir is None:
        preview_dir = find_latest_run_with(workspace, "orderIntents.json")

    if preview_dir:
        st.caption(f"Latest preview: `{preview_dir.name}`")
        intents = _load_json(preview_dir / "orderIntents.json") or []
        if isinstance(intents, list) and intents:
            st.dataframe(orders_table(intents), use_container_width=True)

            st.markdown("### Why was this trade suggested?")
            labels = [
                f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                for o in intents
            ]
            pick = st.selectbox("Choose a trade", labels, key="daily_why_pick")
            idx = labels.index(pick)
            order = intents[idx]

            ctx = load_run_context(preview_dir, workspace)
            exp = explain_order(order, ctx)

            st.info(exp["summary"])

            st.markdown("**Signal checks (what triggered it):**")
            for k, v in exp["signal_checks"].items():
                st.write(("✅ " if v else "❌ ") + k)

            st.markdown("**Filters (basic eligibility):**")
            for k, v in exp["filter_checks"].items():
                st.write(("✅ " if v else "❌ ") + k)

            st.markdown("**Sizing (how big / risk):**")
            st.write(exp["sizing"])

            st.markdown("**Safety checks:**")
            st.write(exp["safety"])

            if advanced:
                st.markdown("**Raw order (advanced):**")
                st.json(exp["raw_order"])

        else:
            st.info("No trade suggestions generated in this preview run.")

        if st.button("Open preview folder", key="daily_open_preview"):
            open_folder(preview_dir)

    st.markdown("---")

    st.markdown("## Step 3 — Place paper orders")
    if not val["ok"]:
        st.warning("Blocked until Safety Check is READY + acknowledged.")
        st.caption(val["reason"])

    if st.button("Place paper orders", disabled=not val["ok"], key="daily_place_orders"):
        ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
        res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=false", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_exec_dir"] = str(res.artifacts_dir)
        st.rerun()

    st.markdown("---")

    st.markdown("## Step 4 — Review results")
    if st.button("Get account snapshot now", key="daily_paper_status"):
        res = run_command("/mfp paper-status", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_status_dir"] = str(res.artifacts_dir)
        st.rerun()

    exec_dir = None
    if "last_exec_dir" in st.session_state:
        p = Path(st.session_state["last_exec_dir"])
        if p.exists():
            exec_dir = p

    if exec_dir:
        st.success(f"Latest execution: `{exec_dir.name}`")
        ack = _load_json(exec_dir / "orders.json") or _load_json(exec_dir / "paper_broker_ack.json") or {}
        st.write("Placed:", len(ack.get("placed", [])))
        st.write("Skipped:", len(ack.get("skipped", [])))
        if st.button("Open execution folder", key="daily_open_exec"):
            open_folder(exec_dir)
        if advanced:
            st.json(ack)

# ======================================================================================
# TAB 1: Backtests
# ======================================================================================
with tabs[1]:
    st.subheader("Backtests")
    st.caption("Use this to test different time periods before running live paper orders.")

    start_default = cfg_current.get("pretrade_check", {}).get("start", "2011-01-01")
    start = st.text_input("Start date (YYYY-MM-DD)", start_default, key="bt_start")
    end = st.text_input("End date (blank = today)", "", key="bt_end")
    max_symbols = st.number_input(
        "Universe size (max symbols)",
        10,
        400,
        int(cfg_current.get("pretrade_check", {}).get("max_symbols", 60)),
        1,
        key="bt_max_symbols",
    )

    if st.button("Run backtest sweep (1d / 1wk / 1mo)", key="bt_run_sweep"):
        end2 = end.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        res = run_command(
            f"/mfp backtest-sweep start={start} end={end2} max_symbols={int(max_symbols)}",
            workspace=workspace,
        )
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_sweep_dir"] = str(res.artifacts_dir)
            if st.button("Open sweep folder", key="bt_open_sweep"):
                open_folder(res.artifacts_dir)

# ======================================================================================
# TAB 2 (Advanced only): Practice Trading
# ======================================================================================
if advanced:
    with tabs[2]:
        st.subheader("Practice Trading (paper)")
        st.caption("Preview anytime. Placing orders is blocked until Safety Check is ready.")

        ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
        val2 = validate_pretrade_certificate(workspace, cfg_current)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Preview trades (no orders)", key="practice_preview"):
                res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=true", workspace=workspace)
                st.markdown(res.summary_md)
        with col2:
            if st.button("Place paper orders", disabled=not val2["ok"], key="practice_place_orders"):
                res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=false", workspace=workspace)
                st.markdown(res.summary_md)

        if not val2["ok"]:
            st.warning("Blocked until Safety Check passes + acknowledged.")
            st.caption(val2["reason"])

# ======================================================================================
# Activity & Files tab index depends on Advanced mode
# ======================================================================================
activity_tab_index = 2 if not advanced else 3
with tabs[activity_tab_index]:
    st.subheader("Activity & Files")
    st.caption("Browse any run folder, open artifacts, and verify integrity (tamper check).")

    runs = list_run_dirs(workspace, limit=200)
    if not runs:
        st.info("No runs found yet.")
    else:
        run_name = st.selectbox("Choose a run folder", [r.name for r in runs], key="act_run_select")
        run_dir = workspace / "reports" / run_name

        if st.button("Open folder in Explorer", key="act_open_folder"):
            open_folder(run_dir)

        integ = verify_manifest(run_dir)
        if integ.get("ok"):
            st.success("Integrity check: PASS ✅")
        else:
            st.error("Integrity check: FAIL ❌")
            if advanced:
                st.json(integ)

        intents = _load_json(run_dir / "orderIntents.json") or []
        if isinstance(intents, list) and intents:
            st.markdown("### Orders")
            st.dataframe(orders_table(intents), use_container_width=True)

            st.markdown("### Why was this trade suggested?")
            labels = [
                f"{o.get('side', '?').upper()} {o.get('symbol')} qty={o.get('qty')} ({o.get('reason', '')})"
                for o in intents
            ]
            pick = st.selectbox("Pick an order to explain", labels, key="act_why_pick")
            idx = labels.index(pick)
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
            st.caption("No orderIntents.json in this run.")

        if advanced:
            st.markdown("---")
            st.markdown("### File viewer (advanced)")
            files = sorted([p for p in run_dir.rglob("*") if p.is_file()], key=lambda x: str(x))
            if not files:
                st.caption("No files in this folder.")
            else:
                rels = [str(p.relative_to(run_dir)) for p in files]
                fsel = st.selectbox("Open a file", rels, key="act_file_select")
                if fsel:
                    fp = run_dir / fsel
                    suf = fp.suffix.lower()
                    if suf == ".json":
                        st.json(_load_json(fp))
                    elif suf in (".md", ".txt"):
                        st.markdown(fp.read_text(encoding="utf-8", errors="ignore"))
                    elif suf == ".csv":
                        try:
                            st.dataframe(pd.read_csv(fp), use_container_width=True)
                        except Exception:
                            st.error("Could not read CSV.")
                    else:
                        st.code(fp.read_text(encoding="utf-8", errors="ignore")[:8000])

# ======================================================================================
# Change Settings tab index depends on Advanced mode
# ======================================================================================
settings_tab_index = 3 if not advanced else 4
with tabs[settings_tab_index]:
    st.subheader("Change Settings (with approvals)")
    st.caption("Flow: Create → Approve → Apply. Unsafe changes are blocked.")

    st.write(f"Current Settings ID: `{_short(settings_id)}`")

    draft = json.loads(json.dumps(cfg_current))  # safe deep copy

    c1, c2, c3 = st.columns(3)
    with c1:
        draft["universe"]["max_symbols"] = st.number_input(
            "Universe size (max symbols)",
            10,
            400,
            int(draft["universe"]["max_symbols"]),
            1,
            key="set_max_symbols",
        )
        draft["proxy"]["ticker"] = st.text_input(
            "Market proxy ticker (IJH/MDY)", str(draft["proxy"]["ticker"]), key="set_proxy"
        )
        draft["signal"]["rsi_buy_below"] = st.number_input(
            "Buy when RSI is below",
            1.0,
            40.0,
            float(draft["signal"]["rsi_buy_below"]),
            1.0,
            key="set_rsi_buy",
        )

    with c2:
        draft["risk"]["max_positions"] = st.number_input(
            "Max positions", 1, 20, int(draft["risk"]["max_positions"]), 1, key="set_max_pos"
        )
        draft["risk"]["risk_per_trade_pct"] = st.number_input(
            "Risk per trade (%)",
            0.01,
            2.0,
            float(draft["risk"]["risk_per_trade_pct"]),
            0.01,
            key="set_risk_trade",
        )
        draft["risk"]["max_open_risk_pct"] = st.number_input(
            "Total open risk cap (%)",
            0.10,
            5.0,
            float(draft["risk"]["max_open_risk_pct"]),
            0.10,
            key="set_open_risk",
        )

    with c3:
        draft["risk"]["stop_atr_mult"] = st.number_input(
            "Stop distance (ATR multiple)",
            0.5,
            5.0,
            float(draft["risk"]["stop_atr_mult"]),
            0.1,
            key="set_stop_atr",
        )
        draft["execution"]["pretrade_check_max_age_days"] = st.number_input(
            "Safety Check expires (days)",
            1,
            30,
            int(draft["execution"]["pretrade_check_max_age_days"]),
            1,
            key="set_cert_age",
        )
        draft["risk"]["scale_in_mode"] = st.selectbox(
            "Scale-in behavior",
            ["none", "confirm_add"],
            index=0,
            key="set_scale_in",
        )

    draft_id = config_hash(draft)
    st.info(f"Draft Settings ID: `{_short(draft_id)}`")

    guard = check_guardrails(draft)
    if guard["ok"]:
        st.success("Safety rules: PASS ✅")
    else:
        st.error("Safety rules: FAIL ❌ (Apply will be blocked)")
        for v in guard["violations"]:
            st.write(f"- ❌ {v['message']}")

    if guard["warnings"]:
        st.warning("Warnings (allowed but review):")
        for w in guard["warnings"]:
            st.write(f"- ⚠️ {w['message']}")

    if st.button("Create Change Request", key="gov_create_proposal"):
        try:
            pr = create_proposal_from_dials(workspace, proposed_cfg=draft, created_by="human")
            st.success(f"Created: {pr['proposal_id']}")
            st.rerun()
        except Exception as e:
            st.error(f"Could not create change request: {type(e).__name__}: {e}")

    proposals = list_proposals(workspace)
    st.markdown("### Change requests")
    if not proposals:
        st.caption("No change requests yet.")
    else:
        dfp = pd.DataFrame(
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
        st.dataframe(dfp, use_container_width=True)

        sel = st.selectbox(
            "Select a change request", [p["proposal_id"] for p in proposals], key="gov_select_proposal"
        )
        pr = load_proposal(workspace, sel)

        st.markdown("#### What will change")
        st.dataframe(pd.DataFrame(pr.get("changes", [])), use_container_width=True)

        colA, colB, colC = st.columns(3)
        with colA:
            if st.button("Approve", key="gov_approve"):
                try:
                    approve_proposal(workspace, sel, approved_by="human")
                    st.success("Approved.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Approve failed: {type(e).__name__}: {e}")

        with colB:
            can_apply = (pr["status"] == "APPROVED") and bool(pr.get("guardrails", {}).get("ok", False))
            if st.button("Apply (make active)", disabled=not can_apply, key="gov_apply"):
                try:
                    apply_proposal(workspace, sel, applied_by="human")
                    st.success("Applied. Settings changed — run Safety Check again.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Apply failed: {type(e).__name__}: {e}")

        with colC:
            if st.button("Reject", key="gov_reject"):
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
        st.dataframe(pd.DataFrame(hist), use_container_width=True)
    else:
        st.caption("No applied changes yet.")

# ======================================================================================
# Prototype tab index depends on Advanced mode
# ======================================================================================
proto_tab_index = 4 if not advanced else 5
with tabs[proto_tab_index]:
    st.subheader("Prototype (embedded HTML)")
    st.caption("Put your file here: prototype/fortress_trading_agent_prototype.html")

    proto = workspace / "prototype" / "fortress_trading_agent_prototype.html"
    if proto.exists():
        html = proto.read_text(encoding="utf-8", errors="ignore")
        components.html(html, height=900, scrolling=True)
        if st.button("Open prototype folder", key="proto_open"):
            open_folder(proto.parent)
    else:
        st.info("Prototype file not found yet.")
