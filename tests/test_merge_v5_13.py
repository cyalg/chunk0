# tests/test_merge_v5_13.py

import logging
import pytest
from workflow_utils.workflow_utils_merge_v5_13 import merge_chunks_v5_13

# --- Stubs / Fixtures ---
# These would normally live in tests/fixtures/, but inline for scaffold

def make_chunk(uuid_suffix, text="original", beats=None, micro_beats=None,
               refs=None, arcs=None):
    return {
        "scene_metadata": {"id": uuid_suffix},
        "scene_uuid": f"scene-{uuid_suffix}",
        "scene_text": text,
        "beats": beats or [{"beat_uuid": f"beat-{uuid_suffix}-1"}],
        "micro_beats": micro_beats or [{"beat_uuid": f"beat-{uuid_suffix}-1"}],
        "refs": refs or {"flag_refs": [f"flag-{uuid_suffix}"]},
        "sections": {"connected_completion_arcs": arcs or []},
    }

@pytest.fixture
def empty_passfile():
    return {}

@pytest.fixture
def schema_stub():
    # minimal schema placeholder
    return {"$schema": "http://json-schema.org/draft-07/schema#"}


# --- Tests ---

def test_add_new_chunk(empty_passfile, schema_stub):
    chunk = make_chunk("A")
    result = merge_chunks_v5_13(empty_passfile, [chunk], schema_stub)
    assert "scene-A" in result
    assert result["scene-A"]["scene_text"] == "original"


def test_duplicate_detection_and_merge(empty_passfile, caplog):
    # Add first chunk
    base = make_chunk("A", text="keep")
    updated = make_chunk("A", text="conflict", refs={"flag_refs": ["flag-B"]})
    result = merge_chunks_v5_13(empty_passfile, [base], None)
    # Merge duplicate with conflict
    caplog.set_level(logging.WARNING)
    result = merge_chunks_v5_13(result, [updated], None)
    # Scene text should not be overwritten
    assert result["scene-A"]["scene_text"] == "keep"
    # Unioned refs
    assert sorted(result["scene-A"]["refs"]["flag_refs"]) == ["flag-A", "flag-B"]
    # Conflict warning should appear
    assert any("CONFLICT" in r.message for r in caplog.records)


def test_force_overwrite_scene_text(empty_passfile, caplog):
    base = make_chunk("A", text="keep")
    updated = make_chunk("A", text="replace")
    caplog.set_level(logging.WARNING)
    result = merge_chunks_v5_13(
        {"scene-A": base}, [updated], None, force_overwrite_text_for=["scene-A"]
    )
    assert result["scene-A"]["scene_text"] == "replace"
    assert any("OVERRIDE" in r.message for r in caplog.records)


def test_union_of_beats_and_micro_beats(empty_passfile):
    base = make_chunk("A", beats=[{"beat_uuid": "beat-1"}])
    updated = make_chunk("A", beats=[{"beat_uuid": "beat-2"}])
    result = merge_chunks_v5_13(empty_passfile, [base, updated], None)
    uuids = [b["beat_uuid"] for b in result["scene-A"]["beats"]]
    assert set(uuids) == {"beat-1", "beat-2"}


def test_chunk15_continuity_arcs_merge(empty_passfile):
    base = make_chunk("15", arcs=["arc-1"])
    updated = make_chunk("15", arcs=["arc-2"])
    result = merge_chunks_v5_13(empty_passfile, [base, updated], None)
    arcs = result["scene-15"]["sections"]["connected_completion_arcs"]
    assert set(arcs) == {"arc-1", "arc-2"}


def test_deterministic_ordering(empty_passfile):
    c1 = make_chunk("A", refs={"flag_refs": ["z", "a"]}, arcs=["b", "a"])
    result = merge_chunks_v5_13(empty_passfile, [c1], None)
    # Refs sorted
    assert result["scene-A"]["refs"]["flag_refs"] == ["a", "z"]
    # Arcs sorted
    assert result["scene-A"]["sections"]["connected_completion_arcs"] == ["a", "b"]
