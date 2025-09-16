import json
from pathlib import Path
from jsonschema import validate as jsonschema_validate, ValidationError
from tests.generate_dummy_chunks import generate_dummy_chunks
from pipeline.pipeline_full import pipeline_full
from workflow.workflow_utils import merge_passfile_chunks
from workflow.workflow_utils_schema import SCHEMA_PATH  # your schema file path

def run_full_pipeline_ci():
    # -----------------------
    # 1. Generate dummy chunks
    # -----------------------
    dummy_chunks = generate_dummy_chunks()
    print(f"Generated {len(dummy_chunks)} dummy chunks.")

    # -----------------------
    # 2. Merge into passfile
    # -----------------------
    passfile_path = Path("data/passfile.json")
    passfile_path.parent.mkdir(exist_ok=True)
    merge_passfile_chunks(dummy_chunks, path=str(passfile_path), overwrite_existing=True)
    print(f"Merged dummy chunks into {passfile_path}")

    # -----------------------
    # 3. Run full pipeline
    # -----------------------
    updated_passfile = pipeline_full(passfile_path=str(passfile_path), chunk_range=range(len(dummy_chunks)))
    print(f"Pipeline processed {len(dummy_chunks)} chunks successfully.")

    # -----------------------
    # 4. Load JSON schema
    # -----------------------
    with open(SCHEMA_PATH) as f:
        schema = json.load(f)

    # -----------------------
    # 5. Assertions for CI/CD + Schema Validation
    # -----------------------
    for idx in range(len(dummy_chunks)):
        chunk_key = f"chunk_{idx}"
        chunk = updated_passfile.get(chunk_key)
        assert chunk is not None, f"{chunk_key} missing from passfile"

        # Scene UUID
        scene_uuid = chunk.get("scene_uuid")
        assert scene_uuid is not None, f"{chunk_key} missing scene_uuid"

        # Beats and beat UUIDs
        beats = chunk.get("beats", [])
        assert len(beats) > 0, f"{chunk_key} has no beats"
        for beat in beats:
            assert beat.get("beat_uuid"), f"{chunk_key} contains a beat without beat_uuid"

        # Trinity advisories
        trinity_advisory = chunk.get("sections", {}).get("trinity_advisory", {})
        cues = trinity_advisory.get("cues", [])
        assert cues, f"{chunk_key} missing Trinity advisory cues"

        # -----------------------
        # JSON Schema validation
        # -----------------------
        try:
            jsonschema_validate(instance=chunk, schema=schema)
        except ValidationError as e:
            raise AssertionError(f"{chunk_key} failed schema validation: {e.message}")

    print("✅ All chunks passed CI/CD assertions and schema validation!")

    # -----------------------
    # 6. Optional: save final passfile for inspection
    # -----------------------
    out_path = Path("dummy_chunks/final_passfile_ci.json")
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(updated_passfile, f, indent=2)
    print(f"Saved final passfile to {out_path}")

# -----------------------
# Execute
# -----------------------
if __name__ == "__main__":
    run_full_pipeline_ci()
