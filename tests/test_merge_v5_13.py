# tests/test_merge_v5_13.py
import json
from pathlib import Path
import logging
import pytest

# Import your real merge function
# from workflow_utils.workflow_utils_merge_v5_13 import merge_chunks_v5_13
# For local dev/demo, alias to a placeholder:
from scripts.generate_fixtures import merge_chunks_local as merge_chunks_v5_13  

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# --------------------------
# Helpers
# --------------------------
def load_json_file(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def make_chunk(uuid_suffix, text="original", beats=None, micro_beats=None,
               refs=None, arcs=None):
    """Stub chunk for quick, isolated tests"""
    return {
        "scene_metadata": {"id": uuid_suffix},
        "scene_uuid": f"scene-{uuid_suffix}",
        "scene_text": text,
        "beats": beats or [{"beat_uuid": f"beat-{uuid_suffix}-1"}],
        "micro_beats": micro_beats or [{"beat_uuid": f"beat-{uuid_suffix}-1"}],
        "refs": refs or {"flag_refs": [f"flag-{uuid_suffix}"]},
        "sections": {"connected_completion_arcs": arcs or []},
    }

# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(scope="module")
def chunk_fixtures():
    """Load all 15 chunk JSON files in numerical order"""
    chunks = []
    for i in range(1, 16):
        path = FIXTURE_DIR / f"chunk_{i:02d}.json"
        chunks.append(load_json_file(path))
    return chunks

@pytest.fixture(scope="module")
def expected_passfile():
    """Load golden merged snapshot"""
    path = FIXTURE_DIR / "expected_passfile.json"
    return load_json_file(path)

@pytest.fixture
def empty_passfile():
    return {}

# --------------------------
# Snapshot / Golden Tests
# --------------------------
def test_merge_against_golden(chunk_fixtures, expected_passfile):
    merged = merge_chunks_v5_13(chunk_fixtures)

    # Top-level scenes
    merged_scenes = set(merged.keys())
    expected_scenes = set(expected_passfile.keys())
    missing_in_merged = expected_scenes - merged_scenes
    extra_in_merged = merged_scenes - expected_scenes
    assert not missing_in_merged, f"Missing scenes in merged output: {missing_in_merged}"
    assert not extra_in_merged, f"Extra scenes in merged output: {extra_in_merged}"

    # Scene-level comparison
    for scene_uuid, expected_scene in expected_passfile.items():
        assert scene_uuid in merged, f"Scene UUID {scene_uuid} missing in merged output"
        merged_scene = merged[scene_uuid]

        # Scene text conflicts
        if merged_scene.get("scene_text") != expected_scene.get("scene_text"):
            conflicts = merged_scene.get("scene_text_conflict", [])
            msg = f"Scene text mismatch for {scene_uuid}. Conflicts: {len(conflicts)}"
            pytest.fail(msg)

        # Beats
        exp_beats = {b["beat_uuid"]: b for b in expected_scene.get("beats", [])}
        merged_beats = {b["beat_uuid"]: b for b in merged_scene.get("beats", [])}
        missing_beats = set(exp_beats) - set(merged_beats)
        extra_beats = set(merged_beats) - set(exp_beats)
        assert not missing_beats, f"Missing beats for {scene_uuid}: {missing_beats}"
        assert not extra_beats, f"Extra beats for {scene_uuid}: {extra_beats}"

        # Micro beats
        exp_mbs = {mb["beat_uuid"]: mb for mb in expected_scene.get("micro_beats", [])}
        merged_mbs = {mb["beat_uuid"]: mb for mb in merged_scene.get("micro_beats", [])}
        missing_mbs = set(exp_mbs) - set(merged_mbs)
        extra_mbs = set(merged_mbs) - set(exp_mbs)
        assert not missing_mbs, f"Missing micro_beats for {scene_uuid}: {missing_mbs}"
        assert not extra_mbs, f"Extra micro_beats for {scene_uuid}: {extra_mbs}"

        # Trinity advisory
        expected_ta = expected_scene["sections"]["trinity_advisory"]
        merged_ta = merged_scene["sections"]["trinity_advisory"]
        for k in ["pearls_detected", "cuffs_detected", "moan_detected",
                  "sexual_actions", "erotic_physiology"]:
            assert set(expected_ta.get(k, [])) == set(merged_ta.get(k, [])), (
                f"Mismatch in trinity advisory '{k}' for {scene_uuid}. "
                f"Expected {expected_ta.get(k, [])}, got {merged_ta.get(k, [])}"
            )
        assert expected_ta.get("two_condition_rule_triggered") == merged_ta.get("two_condition_rule_triggered"), (
            f"two_condition_rule_triggered mismatch for {scene_uuid}"
        )
        assert expected_ta.get("advisory_strength") == merged_ta.get("advisory_strength"), (
            f"advisory_strength mismatch for {scene_uuid}"
        )

        # Connected arcs
        expected_arcs = set(expected_scene["sections"].get("connected_completion_arcs", []))
        merged_arcs = set(merged_scene["sections"].get("connected_completion_arcs", []))
        assert expected_arcs == merged_arcs, f"connected_completion_arcs mismatch for {scene_uuid}"

        # Refs
        for ref_field in ["insert_advisory_refs", "flag_refs", "feedback_summary_ref"]:
            exp_refs = set(expected_scene["refs"].get(ref_field, []))
            merged_refs = set(merged_scene["refs"].get(ref_field, []))
            assert exp_refs == merged_refs, f"Refs mismatch ({ref_field}) for {scene_uuid}"

# --------------------------
# Quick / Stub-style Tests
# --------------------------
def test_add_new_chunk(empty_passfile):
    chunk = make_chunk("A")
    result = merge_chunks_v5_13(empty_passfile, [chunk])
    assert "scene-A" in result
    assert result["scene-A"]["scene_text"] == "original"

def test_duplicate_detection_and_merge(empty_passfile, caplog):
    base = make_chunk("A", text="keep")
    updated = make_chunk("A", text="conflict", refs={"flag_refs": ["flag-B"]})
    result = merge_chunks_v5_13(empty_passfile, [base])
    caplog.set_level(logging.WARNING)
    result = merge_chunks_v5_13(result, [updated])
    assert result["scene-A"]["scene_text"] == "keep"
    assert sorted(result["scene-A"]["refs"]["flag_refs"]) == ["flag-A", "flag-B"]
    assert any("CONFLICT" in r.message for r in caplog.records)

def test_force_overwrite_scene_text(empty_passfile, caplog):
    base = make_chunk("A", text="keep")
    updated = make_chunk("A", text="replace")
    caplog.set_level(logging.WARNING)
    result = merge_chunks_v5_13({"scene-A": base}, [updated], force_overwrite_text_for=["scene-A"])
    assert result["scene-A"]["scene_text"] == "replace"
    assert any("OVERRIDE" in r.message for r in caplog.records)

def test_union_of_beats_and_micro_beats(empty_passfile):
    base = make_chunk("A", beats=[{"beat_uuid": "beat-1"}])
    updated = make_chunk("A", beats=[{"beat_uuid": "beat-2"}])
    result = merge_chunks_v5_13(empty_passfile, [base, updated])
    uuids = [b["beat_uuid"] for b in result["scene-A"]["beats"]]
    assert set(uuids) == {"beat-1", "beat-2"}

def test_chunk15_continuity_arcs_merge(empty_passfile):
    base = make_chunk("15", arcs=["arc-1"])
    updated = make_chunk("15", arcs=["arc-2"])
    result = merge_chunks_v5_13(empty_passfile, [base, updated])
    arcs = result["scene-15"]["sections"]["connected_completion_arcs"]
    assert set(arcs) == {"arc-1", "arc-2"}

def test_deterministic_ordering(empty_passfile):
    c1 = make_chunk("A", refs={"flag_refs": ["z", "a"]}, arcs=["b", "a"])
    result = merge_chunks_v5_13(empty_passfile, [c1])
    # Refs sorted
    assert result["scene-A"]["refs"]["flag_refs"] == ["a", "z"]
    # Arcs sorted
    assert result["scene-A"]["sections"]["connected_completion_arcs"] == ["a", "b"]
