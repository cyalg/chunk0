# workflow_utils_merge_v5_13.py

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------
# Core Merge Utility v5.13
# ---------------------------

def merge_chunks_v5_13(
    existing_passfile: Dict[str, Any],
    incoming_chunks: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]] = None,
    force_overwrite_text_for: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Merge incoming chunks into an existing passfile according to v5.13 rules:
    - Duplicate detection (scene_uuid)
    - Scene_text conflict logging (never overwrite silently)
    - Union of beats, micro_beats, refs, trinity advisory
    - Special handling for Chunk 15 continuity arcs
    """

    force_overwrite_text_for = force_overwrite_text_for or []

    for chunk in incoming_chunks:
        # Step 1: Ensure deterministic UUIDs
        chunk["scene_uuid"] = generate_scene_uuid_from_metadata(
            chunk.get("scene_metadata", {})
        )
        chunk["micro_beats"] = assign_micro_beat_uuids(
            chunk.get("micro_beats", []),
            chunk.get("scene_metadata", {})
        )

        scene_uuid = chunk["scene_uuid"]

        # Step 2: Merge vs Add
        if scene_uuid in existing_passfile:
            existing = existing_passfile[scene_uuid]

            # Beats
            merge_beats_by_uuid(existing, chunk)

            # Micro-beats
            merge_micro_beats_by_uuid(existing, chunk)

            # Refs
            union_and_sort_refs(existing, chunk)

            # Continuity (special for Chunk 15)
            merge_connected_completion_arcs(existing, chunk)

            # Scene text conflict logging
            handle_scene_text_conflict(existing, chunk, force_overwrite_text_for)
        else:
            existing_passfile[scene_uuid] = chunk

        # Step 3: Trinity advisory recompute
        insert_trinity_advisory(existing_passfile[scene_uuid])

    # Step 4: Canonical validation
    if schema:
        validate_minimal_canonical(list(existing_passfile.values()), schema_path=None)

    # Step 5: Deterministic ordering everywhere
    enforce_deterministic_order(existing_passfile)

    return existing_passfile


# ---------------------------
# Sub-Merge Functions
# ---------------------------

def merge_beats_by_uuid(existing: Dict[str, Any], incoming: Dict[str, Any]):
    """Union beats by beat_uuid (dedupe + sort)."""
    existing_beats = {b["beat_uuid"]: b for b in existing.get("beats", [])}
    for b in incoming.get("beats", []):
        existing_beats[b["beat_uuid"]] = b
    existing["beats"] = sorted(existing_beats.values(), key=lambda x: x["beat_uuid"])


def merge_micro_beats_by_uuid(existing: Dict[str, Any], incoming: Dict[str, Any]):
    """Union micro_beats by beat_uuid (dedupe + preserve micro_beat_index continuity)."""
    existing_micro = {mb["beat_uuid"]: mb for mb in existing.get("micro_beats", [])}
    for mb in incoming.get("micro_beats", []):
        existing_micro[mb["beat_uuid"]] = mb
    # Re-enforce index continuity
    merged_list = sorted(existing_micro.values(), key=lambda x: x["beat_uuid"])
    existing["micro_beats"] = enforce_continuity(merged_list, previous_scene_beats=None)


def union_and_sort_refs(existing: Dict[str, Any], incoming: Dict[str, Any]):
    """Union refs arrays deterministically."""
    for ref_key in ["flag_refs", "insert_advisory_refs", "feedback_summary_ref"]:
        merged = set(existing.get("refs", {}).get(ref_key, [])) | set(
            incoming.get("refs", {}).get(ref_key, [])
        )
        existing.setdefault("refs", {})[ref_key] = sorted(merged)


def merge_connected_completion_arcs(existing: Dict[str, Any], incoming: Dict[str, Any]):
    """Special rule for continuity: always merge arcs from Chunk 15."""
    arcs_existing = set(existing.get("sections", {}).get("connected_completion_arcs", []))
    arcs_incoming = set(incoming.get("sections", {}).get("connected_completion_arcs", []))
    merged = arcs_existing | arcs_incoming
    existing.setdefault("sections", {})["connected_completion_arcs"] = sorted(merged)


def handle_scene_text_conflict(
    existing: Dict[str, Any],
    incoming: Dict[str, Any],
    force_overwrite_text_for: List[str],
):
    """Log conflicts; overwrite only if explicitly allowed."""
    scene_uuid = existing["scene_uuid"]
    if existing.get("scene_text") != incoming.get("scene_text"):
        if scene_uuid in force_overwrite_text_for:
            logger.warning(f"[OVERRIDE] Scene text overwritten for {scene_uuid}")
            existing["scene_text"] = incoming["scene_text"]
        else:
            logger.warning(f"[CONFLICT] Scene text mismatch for {scene_uuid}; preserved existing.")


def enforce_deterministic_order(passfile: Dict[str, Any]):
    """Ensure deterministic ordering for reproducible merges."""
    for scene in passfile.values():
        # Beats
        if "beats" in scene:
            scene["beats"] = sorted(scene["beats"], key=lambda x: x["beat_uuid"])
        # Micro-beats
        if "micro_beats" in scene:
            scene["micro_beats"] = sorted(scene["micro_beats"], key=lambda x: x["beat_uuid"])
        # Refs
        if "refs" in scene:
            for k in scene["refs"]:
                scene["refs"][k] = sorted(scene["refs"][k])
        # Arcs
        if "sections" in scene and "connected_completion_arcs" in scene["sections"]:
            scene["sections"]["connected_completion_arcs"] = sorted(
                scene["sections"]["connected_completion_arcs"]
            )
