import json
from pathlib import Path
from tests.generate_dummy_chunks import generate_dummy_chunks
from pipeline.pipeline_full import pipeline_full
from workflow.workflow_utils import merge_passfile_chunks

def run_full_pipeline_test():
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
    # 4. Optional: save final passfile
    # -----------------------
    out_path = Path("dummy_chunks/final_passfile.json")
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(updated_passfile, f, indent=2)
    print(f"Saved final passfile to {out_path}")

    # -----------------------
    # 5. Quick verification
    # -----------------------
    for idx in range(len(dummy_chunks)):
        chunk_key = f"chunk_{idx}"
        scene_uuid = updated_passfile[chunk_key]["scene_uuid"]
        beats_count = len(updated_passfile[chunk_key]["beats"])
        trinity_cues = updated_passfile[chunk_key]["sections"].get("trinity_advisory", {}).get("cues", [])
        print(f"{chunk_key}: scene_uuid={scene_uuid}, beats={beats_count}, trinity_cues={trinity_cues}")

# -----------------------
# Execute
# -----------------------
if __name__ == "__main__":
    run_full_pipeline_test()
