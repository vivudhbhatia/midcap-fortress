from __future__ import annotations

import copy
from pathlib import Path

import pytest

from mfp.config.runtime import DEFAULT_CONFIG, config_hash, load_config, save_config
from mfp.governance.guardrails import check_guardrails
from mfp.governance.proposals import (
    apply_proposal,
    approve_proposal,
    create_proposal_from_dials,
    read_changelog,
)


def test_guardrails_default_ok(tmp_path: Path) -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    res = check_guardrails(cfg)
    assert "ok" in res


def test_proposal_approve_apply_happy_path(tmp_path: Path) -> None:
    ws = tmp_path
    base = copy.deepcopy(DEFAULT_CONFIG)
    save_config(ws, base)
    cur = load_config(ws)
    cur_hash = config_hash(cur)

    # make a small safe change
    draft = copy.deepcopy(cur)
    draft["universe"]["max_symbols"] = int(draft["universe"]["max_symbols"]) - 1

    pr = create_proposal_from_dials(ws, proposed_cfg=draft, created_by="test")
    assert pr["status"] == "DRAFT"
    assert pr["base_config_hash"] == cur_hash

    pr2 = approve_proposal(ws, pr["proposal_id"], approved_by="test")
    assert pr2["status"] == "APPROVED"

    pr3 = apply_proposal(ws, pr["proposal_id"], applied_by="test")
    assert pr3["status"] == "APPLIED"

    hist = read_changelog(ws, limit=10)
    assert len(hist) >= 1


def test_apply_blocked_by_guardrails(tmp_path: Path) -> None:
    ws = tmp_path
    base = copy.deepcopy(DEFAULT_CONFIG)
    save_config(ws, base)

    cur = load_config(ws)
    draft = copy.deepcopy(cur)

    # violate safety rule: rolling dd > 3%
    draft["risk"]["dd_governor"]["enabled"] = True
    draft["risk"]["dd_governor"]["max_dd"] = 0.10

    pr = create_proposal_from_dials(ws, proposed_cfg=draft, created_by="test")
    approve_proposal(ws, pr["proposal_id"], approved_by="test")

    with pytest.raises(ValueError):
        apply_proposal(ws, pr["proposal_id"], applied_by="test")
