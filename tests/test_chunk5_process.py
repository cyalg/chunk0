# -------------------------------
# tests/test_chunk5_process.py
# -------------------------------
import os
import json
import tempfile
import pytest
from jsonschema import validate, ValidationError

from chunk5_process import chunk5_process
from workflow_utils import read_passfile
import schema_passfile  # assumes schema_passfile.py exports SCHEMA dict

# -------------------------------
# Helpers
# -------------------------------
def make_sample_passfile(path: str):
    """Create a minimal sample passfile for testing."""
    sample = {
        "scene_text": "Sophie leans in close, whispering across the desk.",
        "scene_metadata": {
            "scene_uuid": "11111111-1111-1111-1111-111111111111",
            "episode_uuid": "22222222-2222-2222-2222-222222222222"
        },
        "beat_list": [
            {"beat_text": "She adjusts her cuff."},
            {"beat_text": "He looks up, nervous."}
        ],
        "refs": {}
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sample, f, ensure_ascii=False, indent=2)

# -------------------------------
# Tests
# -------------------------------
@pytest.mark.parametrize("mode", ["logic", "llm"])
def test_chunk5_process_modes(mode):
    with tempfile.TemporaryDirectory() as tmpdir:
        pf_path = os.path.join(tmpdir, "passfile.json")
        make_sample_passfile(pf_path)

        # Run chunk5_process in the chosen mode
        updated = chunk5_process(passfile_path=pf_path, mode=mode)

        # Reload passfile to verify persistence
        reloaded = read_passfile(pf_path)

        # Validate schema compliance
        try:
            validate(instance=reloaded, schema=schema_passfile.SCHEMA)
        except ValidationError as e:
            pytest.fail(f"Schema validation failed in mode={mode}: {e}")

        # Core keys must be present
        assert "scene_record" in reloaded
        assert "sequence_marker_analysis" in updated["scene_record"]
        assert "synthetic_seduction_notes" in updated["scene_record"]
        assert "authorial_reflection_notes" in updated["scene_record"]

        # All beats must have beat_uuid
        for beat in reloaded["beat_list"]:
            assert "beat_uuid" in beat and isinstance(beat["beat_uuid"], str)

def test_invalid_mode_raises():
    with tempfile.TemporaryDirectory() as tmpdir:
        pf_path = os.path.join(tmpdir, "passfile.json")
        make_sample_passfile(pf_path)

        with pytest.raises(ValueError):
            chunk5_process(passfile_path=pf_path, mode="not_a_mode")
