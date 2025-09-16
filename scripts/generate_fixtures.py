#!/usr/bin/env python3
# scripts/generate_fixtures.py
"""
Generate fixtures for merge utility v5.13 tests:
- tests/fixtures/chunk_01.json ... chunk_15.json
- tests/fixtures/expected_passfile.json (golden merged snapshot)

Deterministic UUIDs are created via uuid.uuid5 with a fixed namespace.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any
import uuid
import re

# --- Config ---
FIXTURE_DIR = Path("tests") / "fixtures"
FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

TRINITY_TOKENS = {
    "pearls": ["pearl", "necklace", "jewel", "collar", "bead", "gaze"],
    "cuffs": ["cuff", "handcuff", "leather", "strap", "bound", "wrist", "restraint"],
    "moan": ["moan", "gasp", "sigh", "pant", "whimper", "breath", "hitched"]
}
EROTIC_PHYSIOLOGY = ["wet", "slick", "throb", "pulse", "tremor", "arch", "arched"]
SEXUAL_ACTION_KEYWORDS = ["penetrat", "cock", "cum", "oral", "sex", "climax", "orgasm"]

# --- Helpers ---
def make_uuid(name: str) -> str:
    return str(uuid.uuid5(NAMESPACE, name))

def detect_trinity_tokens(text: str) -> Dict[str, List[str]]:
    text_l = (text or "").lower()
    def find(keys):
        found = []
        for k in keys:
            if re.search(rf"\b{re.escape(k)}\b", text_l):
                found.append(k)
        return found
    return {
        "pearls": find(TRINITY_TOKENS["pearls"]),
        "cuffs": find(TRINITY_TOKENS["cuffs"]),
        "moan": find(TRINITY_TOKENS["moan"]),
        "erophys": find(EROTIC_PHYSIOLOGY),
        "sexact": find(SEXUAL_ACTION_KEYWORDS)
    }

def keyword_counts_for_snippet(snippet: str) -> Dict[str, int]:
    counts = {}
    s = snippet.lower()
    counts["pearls"] = sum(s.count(tok) for tok in TRINITY_TOKENS["pearls"])
    counts["cuffs"] = sum(s.count(tok) for tok in TRINITY_TOKENS["cuffs"])
    counts["moan"] = sum(s.count(tok) for tok in TRINITY_TOKENS["moan"])
    return counts

# --- Base manuscript fragments (used to craft chunk texts) ---
MASTER_PARAGRAPHS = [
    "Sophie Vert’s Office, 56th floor of East Midtown, Manhattan. A soft knock echoed, the faint hum of the air conditioner mingling with the distant clatter of heels on marble.",
    "“Come!” Sophie invites the person in, her eyes glued to her reading. The visitor offers a rehearsed smile, uneasy under scrutiny.",
    "“Sit down,” Sophie cut him off mid-sentence, her finger pointed at the chair across her desk. He fidgets; his watch glinted pale in the grey light.",
    "“I… I wanted to ask for…” His words stumbled. Fingers fidgeted, eyes darting to the floor before flinching back failing to meet her glance.",
    "“Please, stop,” she murmured, the edge unmistakable beneath the softness. A private ritual, tugging at the stingray leather, unseen but felt.",
    "Eastman started the usual conversation: a raise and PTO. He had no idea what waited; hope flickered then smothered by her disdain.",
    "“Eastman,” she said quietly, “your output is only meeting the bar.” His jaw tightened; the room smelled faintly of ozone and anxious sweat.",
    "He lists lunches skipped and trivial social wins, trying to make the case. The windows press height against his chest; his breath hitched.",
    "Sophie listened without reaction. Her eyes narrowed, the corner of her mouth lifting in a private, secretive manner."
]

# --- Scene UUIDs mapping (5 logical scenes, each with 3 chunks -> 15 total) ---
SCENE_UUIDS = [
    make_uuid("scene_group_1"),  # chunks 1-3
    make_uuid("scene_group_2"),  # chunks 4-6
    make_uuid("scene_group_3"),  # chunks 7-9
    make_uuid("scene_group_4"),  # chunks 10-12
    make_uuid("scene_group_5"),  # chunks 13-15 (15 = continuity chunk)
]

# helper to create a chunk object
def build_chunk(chunk_index: int, scene_idx: int, paragraph_frag: str, variation: int) -> Dict[str, Any]:
    scene_uuid = SCENE_UUIDS[scene_idx]
    scene = str(scene_idx + 1)
    # create a slightly varied scene_text to allow conflict detection in some chunks
    scene_text = paragraph_frag
    if variation == 1:
        scene_text += " Sophie glances at her phone; a subtle tug of leather is felt."
    elif variation == 2:
        scene_text += " The faint scent of leather and ozone hangs in the air."
    # beats: break snippet into small phrases (1-3 beats)
    words = scene_text.split()
    beats = []
    for i in range(0, min(len(words), 24), 8):
        snippet = " ".join(words[i:i+8])
        beat_uuid = make_uuid(f"{scene_uuid}|beat|{chunk_index}|{i}")
        beats.append({"snippet": snippet, "beat_uuid": beat_uuid})
    # micro_beats mirror beats with keyword counts
    micro_beats = []
    for b in beats:
        counts = keyword_counts_for_snippet(b["snippet"])
        micro_beats.append({
            "beat_uuid": b["beat_uuid"],
            "text": b["snippet"],
            "keyword_counts": counts
        })
    # trinity cues detection for the chunk
    cues = detect_trinity_tokens(scene_text)
    # build trinity advisory (simple)
    trinity_advisory = {
        "pearls_detected": cues["pearls"],
        "cuffs_detected": cues["cuffs"],
        "moan_detected": cues["moan"],
        "sexual_actions": cues["sexact"],
        "erotic_physiology": cues["erophys"],
        "two_condition_rule_triggered": False,
        "advisory_strength": 0
    }
    # compute two_condition_rule_triggered and advisory_strength
    cue_count = sum(bool(trinity_advisory[k]) for k in ["moan_detected", "sexual_actions", "erotic_physiology"])
    trinity_advisory["two_condition_rule_triggered"] = cue_count >= 2
    trinity_advisory["advisory_strength"] = float(cue_count) / 4.0

    # refs: small deterministic insert_advisory_refs sometimes present
    insert_refs = []
    if variation == 2:
        insert_refs = [make_uuid(f"insert_ref|{scene_uuid}|{chunk_index}")]
    flag_refs = []
    if variation == 1 and chunk_index % 2 == 0:
        flag_refs = [f"flag_{chunk_index}"]

    chunk = {
        "scene_metadata": {
            "book_code": "nothing",
            "part": "1",
            "episode": "1",
            "scene": scene,
            "scene_uuid": scene_uuid,
            "scene_title": "Performance Review or Something Like It" if scene_idx == 0 else f"Scene {scene}",
            "concise_summary": (scene_text[:120] + "...") if len(scene_text) > 120 else scene_text,
            "previous_scene": None,
            "next_scene": None
        },
        "scene_text": scene_text,
        "beats": beats,
        "micro_beats": micro_beats,
        "sections": {
            "emotional_arc": {},
            "erotic_arc": {},
            "pacing_strategy_notes": {},
            "connected_completion_arcs": [],  # filled only for chunk 15
            "trinity_advisory": trinity_advisory
        },
        "refs": {
            "scene_uuid": scene_uuid,
            "insert_advisory_refs": insert_refs,
            "flag_refs": flag_refs
        },
        "cross_references": {"previous_scene": None, "next_scene": None},
        "core_identifier": f"nothing_P1E1_S{scene}"
    }
    return chunk

# --- Build 15 chunks ---
chunks: List[Dict[str, Any]] = []
# we will create 5 scenes * 3 chunks each = 15
# For variety: variation cycles [0,1,2] — variation affects small differences & refs
variations = [0, 1, 2]
frag_index = 0
for scene_idx in range(5):
    for chunk_in_scene in range(3):
        chunk_index = scene_idx * 3 + chunk_in_scene + 1
        # pick fragment cycling through MASTER_PARAGRAPHS
        paragraph_frag = MASTER_PARAGRAPHS[frag_index % len(MASTER_PARAGRAPHS)]
        variation = variations[(chunk_in_scene) % len(variations)]
        c = build_chunk(chunk_index, scene_idx, paragraph_frag, variation)
        # set previous/next cross_references to actual scene uuids (simple)
        prev_scene_uuid = SCENE_UUIDS[scene_idx - 1] if scene_idx > 0 else None
        next_scene_uuid = SCENE_UUIDS[scene_idx + 1] if scene_idx < 4 else None
        c["scene_metadata"]["previous_scene"] = prev_scene_uuid
        c["scene_metadata"]["next_scene"] = next_scene_uuid
        c["cross_references"]["previous_scene"] = prev_scene_uuid
        c["cross_references"]["next_scene"] = next_scene_uuid

        # For chunk 15 (the very last chunk), inject continuity arcs referencing earlier scenes:
        if chunk_index == 15:
            c["sections"]["connected_completion_arcs"] = [
                SCENE_UUIDS[0], SCENE_UUIDS[1], SCENE_UUIDS[2]
            ]
            # add some explicit trinity cues to ensure union test
            c["sections"]["trinity_advisory"]["cuffs_detected"].append("restraint")
            c["sections"]["trinity_advisory"]["moan_detected"].append("hitched")
            c["sections"]["trinity_advisory"]["two_condition_rule_triggered"] = True
            c["sections"]["trinity_advisory"]["advisory_strength"] = 2.0

        chunks.append(c)
        frag_index += 1

# --- Write chunk fixtures ---
for i, chunk in enumerate(chunks, start=1):
    name = f"chunk_{i:02d}.json"
    path = FIXTURE_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(chunk, f, indent=2, ensure_ascii=False)

# --- Simple deterministic merge to create expected_passfile.json ---
# Implement merge rules consistent with v5.13:
# - key: scene_uuid
# - beats and micro_beats unioned by beat_uuid (incoming wins fields)
# - refs unioned & sorted
# - trinity advisory arrays unioned & sorted; recompute two_condition_rule_triggered & advisory_strength
# - connected_completion_arcs unioned
def merge_chunks_local(chunks_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for chunk in chunks_list:
        sid = chunk["scene_metadata"]["scene_uuid"]
        # normalize: ensure scene_uuid present
        if sid not in merged:
            # deep-ish copy
            merged[sid] = {
                "scene_metadata": dict(chunk["scene_metadata"]),
                "scene_text": chunk.get("scene_text", ""),
                "beats": list(chunk.get("beats", [])),
                "micro_beats": list(chunk.get("micro_beats", [])),
                "sections": dict(chunk.get("sections", {})),
                "refs": dict(chunk.get("refs", {})),
                "cross_references": dict(chunk.get("cross_references", {})),
                "core_identifier": chunk.get("core_identifier", "")
            }
        else:
            existing = merged[sid]
            # scene_text conflict: preserve existing; if different store conflict entry
            if existing.get("scene_text") != chunk.get("scene_text"):
                existing.setdefault("scene_text_conflict", [])
                existing["scene_text_conflict"].append(chunk.get("scene_text"))
            # merge beats by beat_uuid
            eb = {b["beat_uuid"]: b for b in existing.get("beats", [])}
            for b in chunk.get("beats", []):
                eb[b["beat_uuid"]] = b
            existing["beats"] = sorted(eb.values(), key=lambda x: x["beat_uuid"])
            # merge micro_beats by beat_uuid, merging keyword_counts by max
            em = {mb["beat_uuid"]: mb for mb in existing.get("micro_beats", [])}
            for mb in chunk.get("micro_beats", []):
                if mb["beat_uuid"] in em:
                    # merge keyword_counts with max per key
                    kcounts = dict(em[mb["beat_uuid"]].get("keyword_counts", {}))
                    for k, v in mb.get("keyword_counts", {}).items():
                        kcounts[k] = max(kcounts.get(k, 0), v)
                    em[mb["beat_uuid"]]["keyword_counts"] = kcounts
                    # prefer incoming text
                    em[mb["beat_uuid"]]["text"] = mb.get("text", em[mb["beat_uuid"]].get("text"))
                else:
                    em[mb["beat_uuid"]] = mb
            existing["micro_beats"] = sorted(em.values(), key=lambda x: x["beat_uuid"])
            # merge refs
            for rk in ["insert_advisory_refs", "flag_refs", "feedback_summary_ref"]:
                existing.setdefault("refs", {}).setdefault(rk, [])
                incoming_list = chunk.get("refs", {}).get(rk, [])
                merged_set = set(existing["refs"].get(rk, [])) | set(incoming_list)
                existing["refs"][rk] = sorted(list(merged_set))
            # merge connected arcs
            existing.setdefault("sections", {}).setdefault("connected_completion_arcs", [])
            incoming_arcs = chunk.get("sections", {}).get("connected_completion_arcs", [])
            existing["sections"]["connected_completion_arcs"] = sorted(
                list(set(existing["sections"]["connected_completion_arcs"]) | set(incoming_arcs))
            )
            # merge trinity advisory arrays (union)
            ta = existing.setdefault("sections", {}).setdefault("trinity_advisory", {})
            incoming_ta = chunk.get("sections", {}).get("trinity_advisory", {})
            for k in ["pearls_detected", "cuffs_detected", "moan_detected", "sexual_actions", "erotic_physiology"]:
                old = set(ta.get(k, []))
                new = set(incoming_ta.get(k, []))
                ta[k] = sorted(list(old | new))
            # recompute advisory flags
            cue_count = sum(bool(ta.get(k)) for k in ["moan_detected", "sexual_actions", "erotic_physiology"])
            ta["two_condition_rule_triggered"] = cue_count >= 2
            ta["advisory_strength"] = float(cue_count) / 4.0
            existing["sections"]["trinity_advisory"] = ta
            # merge cross_references where missing
            for ck in ["previous_scene", "next_scene"]:
                if not existing["cross_references"].get(ck) and chunk.get("cross_references", {}).get(ck):
                    existing["cross_references"][ck] = chunk["cross_references"][ck]

    # final ordering deterministic: sort scene entries by UUID
    ordered = dict(sorted(merged.items(), key=lambda kv: kv[0]))
    return ordered

expected = merge_chunks_local(chunks)

# Write expected_passfile.json
expected_path = FIXTURE_DIR / "expected_passfile.json"
with open(expected_path, "w", encoding="utf-8") as f:
    json.dump(expected, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(chunks)} chunk fixtures to {FIXTURE_DIR}")
print(f"Wrote expected merged passfile to {expected_path}")
print("Fixtures generation complete.")
