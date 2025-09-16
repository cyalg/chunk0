# ================================
# pipeline_full.py
# Sequential Chunk Integration (0 → 14)
# Deterministic Scene Ingestion + Micro-Beats/Arcs + Merch Evidence + Trinity Advisory
# Scene Metadata Normalization Added
# ================================

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid

from workflow_utils import (
    read_passfile,
    write_passfile,
    generate_scene_uuid_from_metadata,
    merge_scene_sections,
    deterministic_uuid,
    update_passfile_scene_record,
    insert_trinity_advisory
)
from workflow_utils_marketing import (
    extract_merch_evidence,
    enforce_canonical_merch_refs,
    save_marketing_copy
)
from workflow_utils_schema import SCHEMA_PATH
from jsonschema import validate as jsonschema_validate, ValidationError

# -----------------------
# Logging Config
# -----------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# -----------------------
# Constants / Config
# -----------------------
PASSFILE_PATH = Path("passfile.json")
DEFAULT_CHUNK_SIZE = 5
KEYWORDS = ["dominance", "submission", "tension", "release", "erotic", "gaze", "posture", "voice", "control"]

# -----------------------
# Helpers
# -----------------------
def normalize_scene_metadata(scene_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enforce canonical defaults and fill missing fields.
    Ensures deterministic UUID regeneration if core fields change.
    """
    defaults = {"book_code": "UNK", "part": "1", "episode": "1", "scene": "1"}
    for k, v in defaults.items():
        scene_metadata.setdefault(k, v)
    # Regenerate scene_uuid if missing or invalid
    scene_metadata["scene_uuid"] = safe_generate_scene_uuid(scene_metadata)
    return scene_metadata

def safe_generate_scene_uuid(scene_metadata: Dict[str, Any]) -> str:
    scene_uuid = scene_metadata.get("scene_uuid")
    try:
        if scene_uuid:
            _ = uuid.UUID(scene_uuid)
            return scene_uuid
    except ValueError:
        logging.warning("Invalid scene_uuid detected; regenerating.")
    new_uuid = generate_scene_uuid_from_metadata(scene_metadata)
    logging.info(f"Generated new scene_uuid: {new_uuid}")
    return new_uuid

def assign_beat_uuids_stable(beat_list: List[Dict[str, Any]], scene_metadata: Dict[str, Any]):
    for idx, beat in enumerate(beat_list):
        if not beat.get("beat_uuid"):
            beat["beat_uuid"] = deterministic_uuid(
                book_code=scene_metadata.get("book_code", "UNK"),
                part=scene_metadata.get("part", "1"),
                episode=scene_metadata.get("episode", "1"),
                scene=scene_metadata.get("scene", "1"),
                object_type="beat",
                index=idx
            )
            logging.info(f"Assigned beat_uuid: {beat['beat_uuid']}")

def compute_micro_beats_adaptive(scene_text: str, beat_list: Optional[List[Dict[str, Any]]] = None, chunk_size: int = DEFAULT_CHUNK_SIZE) -> List[Dict[str, Any]]:
    micro_beats = []
    if beat_list:
        for beat in beat_list:
            text = beat.get("text", "")
            counts = {kw: text.lower().count(kw) for kw in KEYWORDS}
            micro_beats.append({"beat_uuid": beat.get("beat_uuid"), "text": text, "keyword_counts": counts})
    else:
        words = scene_text.split()
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i:i + chunk_size])
            counts = {kw: chunk.lower().count(kw) for kw in KEYWORDS}
            micro_beats.append({"beat_uuid": f"synthetic_{i//chunk_size}", "text": chunk, "keyword_counts": counts})
    return micro_beats

def compute_arcs_adaptive(micro_beats: List[Dict[str, Any]]) -> Dict[str, Any]:
    emotional_arc, erotic_arc, pacing_notes = {}, {}, {}
    for beat in micro_beats:
        beat_id = beat["beat_uuid"]
        counts = beat["keyword_counts"]
        total = sum(counts.values()) or 1
        normalized = {k: v / total for k, v in counts.items()}
        emotional_arc[beat_id] = normalized
        erotic_arc[beat_id] = "peak" if normalized.get("erotic", 0) > 0.3 else "build"
        pacing_notes[beat_id] = "fast" if len(beat["text"].split()) > 30 else "steady"
    return {"emotional_arc": emotional_arc, "erotic_arc": erotic_arc, "pacing_strategy_notes": pacing_notes}

def identify_inflection_points_weighted(micro_beats: List[Dict[str, Any]]) -> List[str]:
    points = []
    for i, beat in enumerate(micro_beats):
        score = beat["keyword_counts"].get("erotic", 0) * 2 + beat["keyword_counts"].get("tension", 0)
        if i > 0:
            score += micro_beats[i-1]["keyword_counts"].get("erotic", 0)
        if score > 1:
            points.append(beat["beat_uuid"])
            logging.info(f"Inflection point detected: {beat['beat_uuid']}")
    return points

def package_scene_record(scene_text: str, scene_metadata: Dict[str, Any], arcs: Dict[str, Any], beats: List[Dict[str, Any]]) -> Dict[str, Any]:
    core_id = f"{scene_metadata.get('book_code','UNK')}_P{scene_metadata.get('part','1')}_E{scene_metadata.get('episode','1')}_S{scene_metadata.get('scene','1')}"
    return {
        "core_identifier": core_id,
        "scene_uuid": scene_metadata["scene_uuid"],
        "refs": {
            "scene_uuid": scene_metadata["scene_uuid"],
            "insert_advisory_refs": scene_metadata.get("insert_advisory_refs", []),
            "flag_refs": scene_metadata.get("flag_refs", [])
        },
        "title": scene_metadata.get("scene_title"),
        "concise_summary": scene_metadata.get("concise_summary"),
        "scene_text": scene_text,
        "sections": {
            "emotional_arc": arcs["emotional_arc"],
            "erotic_arc": arcs["erotic_arc"],
            "pacing_strategy_notes": arcs["pacing_strategy_notes"]
        },
        "cross_references": {
            "previous_scene": scene_metadata.get("previous_scene"),
            "next_scene": scene_metadata.get("next_scene")
        },
        "beats": beats
    }

# -----------------------
# Unified Full Pipeline
# -----------------------
def pipeline_full(passfile_path: str = str(PASSFILE_PATH), chunk_range: range = range(0, 15)) -> Dict[str, Any]:
    """
    Sequentially integrate pipeline_chunk0 → pipeline_chunk14 with normalized scene metadata.
    """
    pf = read_passfile(passfile_path)

    for chunk_index in chunk_range:
        logging.info(f"Processing pipeline_chunk{chunk_index}...")

        scene_text = pf.get("scene_text", "")
        scene_metadata = pf.get("scene_metadata", {})

        # -----------------------
        # Normalize Scene Metadata
        # -----------------------
        scene_metadata = normalize_scene_metadata(scene_metadata)
        pf["scene_metadata"] = scene_metadata
        pf["refs"] = enforce_canonical_merch_refs(scene_metadata)

        # Initialize Defaults
        pf.setdefault("beat_list", [])
        pf.setdefault("micro_beats", [])
        pf.setdefault("sections", {})
        pf.setdefault("inflection_points", [])
        pf.setdefault("notes", {})
        pf.setdefault("cross_references", {})

        # Beats & Micro-Beats
        assign_beat_uuids_stable(pf["beat_list"], scene_metadata)
        micro_beats = compute_micro_beats_adaptive(scene_text, pf["beat_list"])
        arcs = compute_arcs_adaptive(micro_beats)
        inflection_points = identify_inflection_points_weighted(micro_beats)

        # Scene Record
        scene_record = package_scene_record(scene_text, scene_metadata, arcs, pf["beat_list"])
        scene_record["micro_beats"] = micro_beats
        scene_record["inflection_points"] = inflection_points

        # Merch Evidence & Trinity Advisory
        merch_evidence = extract_merch_evidence(scene_metadata)
        scene_record = merge_scene_sections(scene_record, {"merch_evidence": merch_evidence})
        scene_record = insert_trinity_advisory(scene_record)

        # Marketing Copy
        save_marketing_copy(scene_metadata["scene_uuid"], merch_evidence, passfile_path)

        # Schema Validation
        try:
            with open(SCHEMA_PATH) as f:
                schema = json.load(f)
            jsonschema_validate(instance=scene_record, schema=schema)
        except ValidationError as e:
            logging.error(f"Schema validation failed in chunk{chunk_index}: {e}")
            raise

        # Update Passfile Safely
        pf["scene_record"] = scene_record
        update_passfile_scene_record(passfile_path, scene_record)

        logging.info(f"Chunk{chunk_index} executed successfully for scene_uuid: {scene_metadata['scene_uuid']}")

    return pf

# -----------------------
# Example Execution
# -----------------------
if __name__ == "__main__":
    updated_passfile = pipeline_full()
