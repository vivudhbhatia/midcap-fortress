from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from mfp.audit.integrity import verify_manifest
from mfp.audit.runlog import read_runlog
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


def list_run_dirs(workspace: Path, limit: int = 80) -> list[Path]:
    reports = workspace / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)
    return runs[:limit]


def read_csv_safe(p: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(p)
    except Exception:
        return None


st.set_page_config(page_title="Midcap Fortress — Control Center", layout="wide")

workspace = workspace_root()
cfg_current = load_config(workspace)
settings_id = config_hash(cfg_current)

# ---------- Sidebar: short glossary ----------
with st.sidebar:
    st.header("Quick glossary (plain English)")
    st.write("**Safety Check**: a backtest sweep + a human acknowledgement. Required before placing trades.")
    st.write(
        "**Settings ID**: a fingerprint of your settings. If it changes, you must re-run the Safety Check."
    )
    st.write("**Preview trades**: shows what would be placed (no orders).")
    st.write("**Activity & Files**: a browseable folder of evidence (JSON/CSV/MD/ZIP).")

st.title("Midcap Fortress — Control Center")
st.caption("Designed for transparency: every run writes files you can inspect, hash, and archive.")

tabs = st.tabs(
    [
        "Start Here",
        "Test Strategy",
        "Practice Trading",
        "Activity & Files",
        "Change Settings (with approvals)",
        "Prototype",
    ]
)

# ---------------- Start Here ----------------
with tabs[0]:
    st.subheader("Today’s checklist")

    colA, colB, colC = st.columns([1.1, 1.2, 1.7])

    with colA:
        st.markdown("### 1) Account snapshot")
        if st.button("Get account snapshot now"):
            res = run_command("/mfp paper-status", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir and st.button("Open snapshot files"):
                open_folder(res.artifacts_dir)

    with colB:
        st.markdown("### 2) Safety Check (required)")
        val = validate_pretrade_certificate(workspace, cfg_current)
        cert = load_pretrade_certificate(workspace)

        if val["ok"]:
            st.success("Safety Check: PASS ✅")
        else:
            st.warning("Safety Check: NOT READY ⚠️")
        st.caption(f"Reason: {val['reason']}")

        st.write(f"Settings ID: `{_short(settings_id)}`")

        if cert:
            st.caption("Latest Safety Check record:")
            st.json(
                {
                    k: cert.get(k)
                    for k in ["created_ts_utc", "pass", "reviewed", "config_hash", "sweep_out_dir"]
                }
            )

        if st.button("I reviewed the Safety Check results (acknowledge)"):
            res = run_command("/mfp pretrade-ack", workspace=workspace)
            st.markdown(res.summary_md)
            st.rerun()

    with colC:
        st.markdown("### 3) Next actions")
        st.info("Recommended flow: **Run Safety Check → Preview trades → Place trades (paper)**")

        if st.button("Run Safety Check now (backtest sweep)"):
            start = cfg_current.get("pretrade_check", {}).get("start", "2011-01-01")
            end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            max_symbols = int(cfg_current.get("pretrade_check", {}).get("max_symbols", 60))
            res = run_command(
                f"/mfp backtest-sweep start={start} end={end} max_symbols={max_symbols}", workspace=workspace
            )
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open Safety Check files", on_click=open_folder, args=(res.artifacts_dir,))

        st.markdown("---")
        st.markdown("#### Recent activity")
        items = read_runlog(workspace, limit=30)
        if items:
            st.dataframe(pd.DataFrame(items))
        else:
            st.caption("No runs yet. Once you run something, you’ll see it here.")

# ---------------- Test Strategy ----------------
with tabs[1]:
    st.subheader("Test Strategy (Backtests)")
    st.caption("This is how you build confidence before placing any trades.")

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

    st.info("Tip: After reviewing results, go back to **Start Here** and click the acknowledge button.")

# ---------------- Practice Trading ----------------
with tabs[2]:
    st.subheader("Practice Trading (Paper)")
    st.caption(
        "You can always preview. Placing paper orders requires a passing Safety Check + acknowledgement."
    )

    ms = int(cfg_current.get("universe", {}).get("max_symbols", 60))
    val = validate_pretrade_certificate(workspace, cfg_current)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Preview trades (no orders)")
        if st.button("Preview trades now"):
            res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=true", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open preview files", on_click=open_folder, args=(res.artifacts_dir,))

    with col2:
        st.markdown("### Place paper orders")
        if not val["ok"]:
            st.warning("Blocked until Safety Check is ready.")
            st.caption(val["reason"])

        if st.button("Place paper orders", disabled=not val["ok"]):
            res = run_command(f"/mfp paper-cycle max_symbols={ms} dry_run=false", workspace=workspace)
            st.markdown(res.summary_md)
            if res.artifacts_dir:
                st.button("Open execution files", on_click=open_folder, args=(res.artifacts_dir,))

    st.markdown("---")
    st.markdown("### Stops / protection")
    if st.button("Reconcile + place protective stops"):
        res = run_command(f"/mfp paper-reconcile max_symbols={ms} place_stops=true", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.button("Open reconcile files", on_click=open_folder, args=(res.artifacts_dir,))

# ---------------- Activity & Files ----------------
with tabs[3]:
    st.subheader("Activity & Files")
    st.caption("Browse any run folder, open artifacts, and verify file integrity (tamper check).")

    runs = list_run_dirs(workspace, limit=100)
    if not runs:
        st.info("No runs found yet. Run a Safety Check or Preview trades to generate files.")
    else:
        run_name = st.selectbox("Choose a run folder", [r.name for r in runs])
        run_dir = workspace / "reports" / run_name

        st.write("Folder:", str(run_dir))
        if st.button("Open in Explorer"):
            open_folder(run_dir)

        integ = verify_manifest(run_dir)
        if integ.get("ok"):
            st.success("Integrity check: PASS ✅ (files match manifest hashes)")
        else:
            st.error("Integrity check: FAIL ❌")
            st.json(integ)

        files = sorted([p for p in run_dir.glob("*") if p.is_file()], key=lambda x: x.name)
        fsel = st.selectbox("Open a file", [p.name for p in files])
        fp = run_dir / fsel

        if fp.suffix.lower() == ".json":
            st.json(json.loads(fp.read_text(encoding="utf-8")))
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

# ---------------- Change Settings (with approvals) ----------------
with tabs[4]:
    st.subheader("Change Settings (with approvals)")
    st.caption("Changes flow: Create Change Request → Approve → Apply. Unsafe changes are blocked.")

    st.markdown(f"**Current Settings ID:** `{_short(settings_id)}`")

    draft = copy.deepcopy(cfg_current)

    st.markdown("### Draft settings (edit here)")
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
        st.error("Safety rules: FAIL ❌ (this will be blocked from Apply)")
        for v in guard["violations"]:
            st.write(f"- ❌ {v['message']}")

    if guard["warnings"]:
        st.warning("Warnings (allowed, but consider reviewing):")
        for w in guard["warnings"]:
            st.write(f"- ⚠️ {w['message']}")

    st.markdown("---")
    if st.button("Create Change Request"):
        try:
            pr = create_proposal_from_dials(workspace, proposed_cfg=draft, created_by="human")
            st.success(f"Created Change Request: {pr['proposal_id']}")
            st.rerun()
        except Exception as e:
            st.error(f"Could not create change request: {type(e).__name__}: {e}")

    st.markdown("### Pending change requests")
    proposals = list_proposals(workspace)
    if not proposals:
        st.caption("No change requests yet.")
    else:
        table = []
        for p in proposals:
            table.append(
                {
                    "proposal_id": p["proposal_id"],
                    "status": p["status"],
                    "from_settings_id": _short(p["base_config_hash"]),
                    "to_settings_id": _short(p["proposed_config_hash"]),
                    "guardrails_ok": bool(p.get("guardrails", {}).get("ok", False)),
                    "created_ts_utc": p.get("created_ts_utc"),
                }
            )
        st.dataframe(pd.DataFrame(table))

        sel = st.selectbox("Select a change request", [p["proposal_id"] for p in proposals])
        pr = load_proposal(workspace, sel)

        st.markdown("#### Details")
        st.write("Status:", pr["status"])
        st.write("From Settings ID:", _short(pr["base_config_hash"]))
        st.write("To Settings ID:", _short(pr["proposed_config_hash"]))

        st.markdown("##### What will change")
        st.dataframe(pd.DataFrame(pr.get("changes", [])))

        st.markdown("##### Safety rules result")
        st.write("PASS:", bool(pr.get("guardrails", {}).get("ok", False)))
        if pr.get("guardrails", {}).get("violations"):
            st.error("Violations:")
            for v in pr["guardrails"]["violations"]:
                st.write(f"- ❌ {v['message']}")
        if pr.get("guardrails", {}).get("warnings"):
            st.warning("Warnings:")
            for w in pr["guardrails"]["warnings"]:
                st.write(f"- ⚠️ {w['message']}")

        cA, cB, cC = st.columns(3)

        with cA:
            if st.button("Approve"):
                try:
                    approve_proposal(workspace, sel, approved_by="human")
                    st.success("Approved.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Approve failed: {type(e).__name__}: {e}")

        with cB:
            can_apply = (pr["status"] == "APPROVED") and bool(pr.get("guardrails", {}).get("ok", False))
            if st.button("Apply (make active)", disabled=not can_apply):
                try:
                    apply_proposal(workspace, sel, applied_by="human")
                    st.success("Applied. Your Settings ID changed, so you should re-run the Safety Check.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Apply failed: {type(e).__name__}: {e}")

        with cC:
            if st.button("Reject"):
                try:
                    reject_proposal(workspace, sel, rejected_by="human", reason="Rejected in UI")
                    st.success("Rejected.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Reject failed: {type(e).__name__}: {e}")

    st.markdown("---")
    st.markdown("### Change History (Settings ID lineage)")
    hist = read_changelog(workspace, limit=200)
    if not hist:
        st.caption("No applied changes yet.")
    else:
        st.dataframe(pd.DataFrame(hist))

# ---------------- Prototype ----------------
with tabs[5]:
    st.subheader("Prototype (embedded HTML)")
    st.caption("Place your prototype file at: prototype/fortress_trading_agent_prototype.html")

    proto = workspace / "prototype" / "fortress_trading_agent_prototype.html"
    if proto.exists():
        html = proto.read_text(encoding="utf-8", errors="ignore")
        components.html(html, height=900, scrolling=True)
        st.button("Open prototype folder", on_click=open_folder, args=(proto.parent,))
    else:
        st.info("Prototype file not found yet.")
