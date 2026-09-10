from __future__ import annotations

import inspect
from pathlib import Path

import dagster as dg
import pytest

from mushroom.definitions import defs
from mushroom.defs.sync import _live_target, synchronization_results
from mushroom.kernel import LocalArtifactStore


def test_prepare_sync_remains_fixture_only(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))

    result = defs().resolve_job_def("prepare_sync").execute_in_process()

    assert result.success
    assert LocalArtifactStore(tmp_path).resolve("operations")[1]


def test_prepare_fresh_sync_acquires_before_planning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))
    monkeypatch.setattr("project.acquisition.acquire", lambda: __import__("project.domain", fromlist=["source_fixture"]).source_fixture().read_bytes())

    result = defs().resolve_job_def("prepare_fresh_sync").execute_in_process()

    assert result.success
    assert LocalArtifactStore(tmp_path).resolve("source-origin")[1] == b'{"source":"public-sources"}\n'


def test_validate_sync_requires_an_enabled_client(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MUSHROOM_HOME", str(tmp_path))

    result = defs().resolve_job_def("validate_sync").execute_in_process(
        resources={"warmhub_client": None}, raise_on_error=False
    )

    assert not result.success
    assert result.failure_data_for_node("current_state") is not None


@pytest.mark.parametrize("target", [None, "example/mushroom-starter"])
def test_live_jobs_require_an_explicit_nonstarter_target(monkeypatch, target: str | None) -> None:
    if target is None:
        monkeypatch.delenv("MUSHROOM_TARGET", raising=False)
    else:
        monkeypatch.setenv("MUSHROOM_TARGET", target)

    with pytest.raises(dg.Failure, match="explicit non-starter MUSHROOM_TARGET"):
        _live_target()


def test_synchronize_does_not_reuse_advisory_validation() -> None:
    job = defs().resolve_job_def("synchronize")

    assert "server_validation" not in job.graph.node_names()
    assert [input_def.name for input_def in job.graph.node_dict["synchronization_results"].definition.input_defs] == [
        "operation_plan"
    ]
    assert "validate_plan(" not in inspect.getsource(
        synchronization_results.node_def.compute_fn.decorated_fn
    )
