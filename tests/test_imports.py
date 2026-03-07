def test_imports_and_exports():
    import mfp.cli  # noqa: F401
    from mfp.audit.evidence import create_evidence_zip
    from mfp.paper.paper_cycle import paper_status
    from mfp.ui.github_opsbot import run_command

    assert callable(create_evidence_zip)
    assert callable(paper_status)
    assert callable(run_command)
