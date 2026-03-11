from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from mfp.audit.integrity import verify_manifest
from mfp.config.normalize import normalize_config
from mfp.config.runtime import config_hash, load_config
from mfp.governance.guardrails import check_guardrails
from mfp.governance.portfolio_safety import load_certificate, validate_certificate
from mfp.governance.proposals import (
    apply_proposal,
    approve_proposal,
    create_proposal_from_dials,
    list_proposals,
    load_proposal,
    read_changelog,
    reject_proposal,
)
from mfp.objectives.targets import expected_cum_return, window_days
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


st.set_page_config(page_title="Midcap Fortress — Portfolio Control Center", layout="wide")

workspace = workspace_root()
cfg = normalize_config(load_config(workspace))
settings_id = config_hash(cfg)

with st.sidebar:
    st.header("Mode")
    advanced = st.toggle("Advanced view", value=False, key="mode_adv")
    st.markdown("---")
    st.write("**Safety Check** = backtest evidence + your acknowledgement.")
    st.write("**Goal** = long-run annual growth target; short windows are informational.")
    st.write("**Max drop** = rolling drawdown guardrails (daily-based).")

st.title("Midcap Fortress — Portfolio Control Center")
st.caption("Portfolio-first. Every action produces evidence files you can inspect and verify.")

tabs = st.tabs(
    ["Daily Flow", "Portfolio Settings", "Backtests", "Activity & Files", "Governance", "Prototype"]
)

# ---------------- Daily Flow ----------------
with tabs[0]:
    st.subheader("Daily Flow (one screen)")
    st.write(f"Settings ID: `{_short(settings_id)}`")

    cert = load_certificate(workspace)
    val = validate_certificate(workspace, cfg, require_reviewed=False)

    st.markdown("## Step 1 — Portfolio Safety Check")
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Run Safety Check now", key="daily_run_safety"):
            start = cfg.get("pretrade_check", {}).get("start", "2011-01-01")
            end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            res = run_command(f"/mfp portfolio-safety-check start={start} end={end}", workspace=workspace)
            st.markdown(res.summary_md)
            st.rerun()
    with c2:
        if val["ok"]:
            st.success("Safety Check exists ✅")
        else:
            st.warning("Safety Check missing/invalid ⚠️")
        st.caption(f"Reason: {val['reason']}")

    st.markdown("## Step 2 — Acknowledge you reviewed results")
    if st.button("I reviewed the Safety Check (acknowledge)", key="daily_ack"):
        res = run_command("/mfp portfolio-safety-ack", workspace=workspace)
        st.markdown(res.summary_md)
        st.rerun()

    st.markdown("---")
    st.markdown("## Step 3 — Preview portfolio trades (no orders)")
    if st.button("Preview trades", key="daily_preview"):
        res = run_command("/mfp portfolio-preview", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_portfolio_run"] = str(res.artifacts_dir)
        st.rerun()

    last_run = (
        Path(st.session_state["last_portfolio_run"]) if "last_portfolio_run" in st.session_state else None
    )
    if last_run and last_run.exists():
        orders_path = last_run / "portfolio" / "portfolio_orders.json"
        orders = _load_json(orders_path) or []
        if isinstance(orders, list) and orders:
            st.dataframe(pd.DataFrame(orders), use_container_width=True)
        else:
            st.info("No orders suggested in latest preview.")
        if st.button("Open preview folder", key="daily_open_preview"):
            open_folder(last_run)

    st.markdown("---")
    st.markdown("## Step 4 — Place paper orders")
    val_place = validate_certificate(workspace, cfg, require_reviewed=True)
    if not val_place["ok"]:
        st.warning(f"Blocked until Safety Check is acknowledged. Reason: {val_place['reason']}")

    if st.button("Place paper orders", disabled=not val_place["ok"], key="daily_place"):
        res = run_command("/mfp portfolio-place", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_portfolio_exec"] = str(res.artifacts_dir)
        st.rerun()

    st.markdown("---")
    st.markdown("## Step 5 — Run Critic + Switch Suggestion")
    c3, c4 = st.columns(2)
    with c3:
        if st.button("Run critic now", key="daily_critic"):
            res = run_command("/mfp critic-run", workspace=workspace)
            st.markdown(res.summary_md)
    with c4:
        if st.button("Suggest switching", key="daily_switch"):
            res = run_command("/mfp switch-suggest", workspace=workspace)
            st.markdown(res.summary_md)

# ---------------- Portfolio Settings ----------------
with tabs[1]:
    st.subheader("Portfolio Settings (dials)")
    draft = json.loads(json.dumps(cfg))  # deep copy

    st.markdown("### Goal (plain English)")
    c1, c2, c3 = st.columns(3)
    with c1:
        draft["objectives"]["target_annual_cagr"] = st.number_input(
            "Target growth per year (e.g., 0.20 = 20%)",
            0.0,
            1.0,
            float(draft["objectives"]["target_annual_cagr"]),
            0.01,
            key="obj_target",
        )
    with c2:
        draft["objectives"]["goal_horizon_years"] = st.number_input(
            "Goal horizon (years)",
            1,
            10,
            int(draft["objectives"]["goal_horizon_years"]),
            1,
            key="obj_horizon",
        )
    with c3:
        draft["objectives"]["acceptance_factor"] = st.number_input(
            "Acceptance factor (soften thresholds)",
            0.1,
            1.0,
            float(draft["objectives"]["acceptance_factor"]),
            0.05,
            key="obj_accept",
        )

    st.markdown("### Max drop (rolling guardrails)")
    d1, d2, d3 = st.columns(3)
    with d1:
        draft["objectives"]["drawdown_limits"]["1D"] = st.number_input(
            "Max 1‑day drop", 0.0, 0.2, float(draft["objectives"]["drawdown_limits"]["1D"]), 0.001, key="dd1"
        )
    with d2:
        draft["objectives"]["drawdown_limits"]["5D"] = st.number_input(
            "Max 1‑week drop (5D)",
            0.0,
            0.3,
            float(draft["objectives"]["drawdown_limits"]["5D"]),
            0.001,
            key="dd5",
        )
    with d3:
        draft["objectives"]["drawdown_limits"]["20D"] = st.number_input(
            "Max 1‑month drop (20D)",
            0.0,
            0.5,
            float(draft["objectives"]["drawdown_limits"]["20D"]),
            0.001,
            key="dd20",
        )

    st.markdown("### Portfolio size + allocations")
    draft["portfolio"]["paper_equity_usd"] = st.number_input(
        "Virtual paper portfolio size (USD)",
        1000.0,
        10000000.0,
        float(draft["portfolio"]["paper_equity_usd"]),
        1000.0,
        key="port_eq",
    )

    # allocations
    strat = draft["portfolio"]["strategies"]
    st.caption("Adjust allocations. They must sum to 100% to pass guardrails.")
    alloc_total = 0.0
    for sid, spec in strat.items():
        if not spec.get("enabled", True):
            continue
        spec["allocation_pct"] = st.slider(
            f"{sid} — {spec.get('label', '')}",
            0.0,
            1.0,
            float(spec.get("allocation_pct", 0.0)),
            0.01,
            key=f"alloc_{sid}",
        )
        alloc_total += float(spec["allocation_pct"])
    st.info(f"Allocation sum: {alloc_total:.4f} (must be 1.0000)")

    st.markdown("### What your goal implies in each window")
    target = float(draft["objectives"]["target_annual_cagr"])
    acceptance = float(draft["objectives"]["acceptance_factor"])
    rows = []
    for w in draft["objectives"]["return_windows"]:
        days = window_days(w)
        exp = expected_cum_return(target, days)
        thr = exp * acceptance
        rows.append({"window": w, "days": days, "expected_cum": exp, "threshold_cum": thr})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.markdown("---")
    guard = check_guardrails(draft)
    if guard["ok"]:
        st.success("Guardrails: PASS ✅")
    else:
        st.error("Guardrails: FAIL ❌")
        for v in guard["violations"]:
            st.write(f"- ❌ {v['message']}")

    if guard["warnings"]:
        st.warning("Warnings:")
        for w in guard["warnings"]:
            st.write(f"- ⚠️ {w['message']}")

    if st.button("Create Change Request", key="create_proposal"):
        pr = create_proposal_from_dials(workspace, proposed_cfg=draft, created_by="human")
        st.success(f"Created proposal: {pr['proposal_id']}")
        st.rerun()

# ---------------- Backtests ----------------
with tabs[2]:
    st.subheader("Backtests (per strategy)")
    st.caption("Run backtests before you place trades.")

    # choose strategy
    strat = cfg["portfolio"]["strategies"]
    sid = st.selectbox("Strategy", list(strat.keys()), key="bt_sid")
    spec = strat[sid]
    universe = (
        "custom:" + ",".join(spec.get("symbols", []))
        if spec.get("symbols")
        else spec.get("universe", "sp400")
    )

    start = st.text_input(
        "Start (YYYY-MM-DD)", cfg.get("pretrade_check", {}).get("start", "2011-01-01"), key="bt_start"
    )
    end = st.text_input("End (YYYY-MM-DD)", datetime.now(timezone.utc).strftime("%Y-%m-%d"), key="bt_end")

    if st.button("Run strategy safety sweep (1d/1wk/1mo)", key="bt_run"):
        res = run_command(f"/mfp portfolio-safety-check start={start} end={end}", workspace=workspace)
        st.markdown(res.summary_md)

    st.info(f"Universe for {sid}: `{universe}` (based on settings)")

# ---------------- Activity & Files ----------------
with tabs[3]:
    st.subheader("Activity & Files")
    reports = workspace / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)

    if not runs:
        st.info("No runs yet.")
    else:
        run_name = st.selectbox("Run folder", [r.name for r in runs], key="act_run")
        run_dir = reports / run_name

        if st.button("Open folder", key="act_open"):
            open_folder(run_dir)

        integ = verify_manifest(run_dir)
        if integ.get("ok"):
            st.success("Integrity: PASS ✅")
        else:
            st.error("Integrity: FAIL ❌")
            if advanced:
                st.json(integ)

        files = sorted([p for p in run_dir.rglob("*") if p.is_file()], key=lambda x: str(x))
        if files:
            rels = [str(p.relative_to(run_dir)) for p in files]
            fsel = st.selectbox("Open file", rels, key="act_file")
            if fsel:
                fp = run_dir / fsel
                if fp.suffix.lower() == ".json":
                    st.json(_load_json(fp))
                else:
                    st.code(fp.read_text(encoding="utf-8", errors="ignore")[:8000])
        else:
            st.info("No files in this run.")

# ---------------- Governance ----------------
with tabs[4]:
    st.subheader("Governance (Approve → Apply)")
    proposals = list_proposals(workspace)
    if not proposals:
        st.info("No proposals yet. Create one from Portfolio Settings.")
    else:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "proposal_id": p["proposal_id"],
                        "status": p["status"],
                        "from": _short(p["base_config_hash"]),
                        "to": _short(p["proposed_config_hash"]),
                        "safety_ok": bool(p.get("guardrails", {}).get("ok", False)),
                    }
                    for p in proposals
                ]
            ),
            use_container_width=True,
        )

        sel = st.selectbox("Select proposal", [p["proposal_id"] for p in proposals], key="gov_sel")
        pr = load_proposal(workspace, sel)

        st.markdown("### Changes")
        st.dataframe(pd.DataFrame(pr.get("changes", [])), use_container_width=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Approve", key="gov_approve"):
                approve_proposal(workspace, sel, approved_by="human")
                st.success("Approved.")
                st.rerun()
        with c2:
            can_apply = pr["status"] == "APPROVED" and bool(pr.get("guardrails", {}).get("ok", False))
            if st.button("Apply", disabled=not can_apply, key="gov_apply"):
                apply_proposal(workspace, sel, applied_by="human")
                st.success("Applied. Re-run Safety Check before placing orders.")
                st.rerun()
        with c3:
            if st.button("Reject", key="gov_reject"):
                reject_proposal(workspace, sel, rejected_by="human", reason="Rejected in UI")
                st.success("Rejected.")
                st.rerun()

    st.markdown("---")
    st.markdown("### Change history")
    hist = read_changelog(workspace, limit=200)
    if hist:
        st.dataframe(pd.DataFrame(hist), use_container_width=True)
    else:
        st.caption("No applied changes yet.")

# ---------------- Prototype ----------------
with tabs[5]:
    st.subheader("Prototype")
    proto = workspace / "prototype" / "fortress_trading_agent_prototype.html"
    if proto.exists():
        import streamlit.components.v1 as components

        components.html(proto.read_text(encoding="utf-8", errors="ignore"), height=900, scrolling=True)
    else:
        st.info("Prototype file not found. Put it at: prototype/fortress_trading_agent_prototype.html")
