import random
import json
from pathlib import Path
from workflow.workflow_utils import (
    merge_passfile_chunks,
    assign_micro_beat_uuids,
    insert_trinity_advisory,
    read_passfile,
    write_passfile
)

# -----------------------
# Dummy Chunk Generator
# -----------------------
def generate_dummy_chunks(num_chunks: int = 14):
    TRINITY_KEYWORDS = {
        "pearls": ["pearl", "necklace", "jewel", "collar", "bead"],
        "cuffs": ["cuff", "leather", "bound", "tie", "wrist"],
        "moan": ["moan", "gasp", "sigh", "pant", "whimper"]
    }

    chunks = []
    for i in range(1, num_chunks + 1):
        part = str((i - 1) // 3 + 1)
        episode = str((i - 1) % 3 + 1)
        scene = str(i)

        beats = []
        for j in range(1, random.randint(2, 4)):
            keyword_type = random.choice(list(TRINITY_KEYWORDS.keys()))
            keyword = random.choice(TRINITY_KEYWORDS[keyword_type])
            snippet = f"Micro-beat {i}-{j}: {keyword} appears and triggers action."
            beats.append({"snippet": snippet})

        scene_text = " ".join(b["snippet"] for b in beats)

        chunk = {
            "scene_metadata": {
                "book_code": "TESTBOOK",
                "part": part,
                "episode": episode,
                "scene": scene
            },
            "scene_text": scene_text,
            "beats": beats
        }
        chunks.append(chunk)
    return chunks

# -----------------------
# Run Merge & Pipeline Test
# -----------------------
def run_merge_pipeline_test():
    dummy_chunks = generate_dummy_chunks()
    passfile_path = Path("data/passfile.json")
    passfile_path.parent.mkdir(exist_ok=True)

    # Merge chunks into passfile
    merge_passfile_chunks(dummy_chunks, path=str(passfile_path), overwrite_existing=True)
    print(f"Merged {len(dummy_chunks)} chunks into {passfile_path}")

    # Read merged passfile
    pf = read_passfile(str(passfile_path))

    # Assign micro-beat UUIDs & insert trinity advisories per scene
    for scene_uuid, scene_record in pf.items():
        beats = scene_record.get("beats", [])
        metadata = scene_record.get("scene_metadata", {})
        beats_with_uuids = assign_micro_beat_uuids(beats, metadata)
        scene_record["beats"] = beats_with_uuids
        scene_record = insert_trinity_advisory(scene_record)
        pf[scene_uuid] = scene_record

    # Save updated passfile
    write_passfile(pf, path=str(passfile_path), overwrite=True)
    print(f"Assigned micro-beat UUIDs and inserted trinity advisories for all scenes.")

    # Optional: print summary
    for scene_uuid, scene_record in pf.items():
        print(f"Scene {scene_record['scene_metadata']['scene']} | Beats: {len(scene_record['beats'])} | "
              f"Trinity cues: {scene_record['sections'].get('trinity_advisory', {}).get('cues', 'N/A')}")

# -----------------------
# Execute
# -----------------------
if __name__ == "__main__":
    run_merge_pipeline_test()
