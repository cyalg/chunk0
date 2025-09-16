# -----------------------
# workflow_utils v5.9 - Full Production
# Added: enforce cross-scene continuity in enforce_continuity
# -----------------------
import re, uuid, json, logging, tempfile, shutil
from pathlib import Path
from copy import deepcopy
from typing import List, Dict, Any, Optional, Union
from jsonschema import validate as jsonschema_validate, ValidationError

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# -----------------------
# Constants
# -----------------------
PASSFILE_PATH = Path("passfile.json")
SCHEMA_PATH = Path("schema_passfile.json")
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")
_WS_RE = re.compile(r"\s+")
_ALNUM_RE = re.compile(r"[^a-z0-9]")

TRINITY_TOKENS = {
    "pearls": ["pearl","necklace","jewel","sheen","collar","cufflink","bead","lustre"],
    "cuffs": ["cuff","handcuff","leather","strap","bound","tie","wrist","grip","restrain","locked"],
    "moan": ["moan","gasp","sigh","release","climax","orgasm","whimper","pant"]
}
SEXUAL_ACTION_KEYWORDS = ["penetrat","cock","cum","oral","sex","climax","orgasm"]
EROTIC_PHYSIOLOGY = ["wet","slick","throb","pulse","tremor","arch","arched"]

# -----------------------
# Utilities (unchanged)
# -----------------------
def _normalize_key(s: Optional[str]) -> str:
    s = (s or "").strip()
    s = _WS_RE.sub(" ", s).lower()
    s = _ALNUM_RE.sub("", s)
    return s

def deterministic_uuid(book_code: str,
                       part: str,
                       episode: str,
                       scene: str,
                       object_type: str,
                       array_index: Optional[int] = None,
                       auto_index: Optional[int] = None) -> str:
    key = f"{_normalize_key(book_code)}|P{_normalize_key(str(part))}|E{_normalize_key(str(episode))}|S{_normalize_key(str(scene))}|{_normalize_key(object_type)}"
    if array_index is not None: key += f"|A{array_index}"
    if auto_index is not None: key += f"|IDX{auto_index}"
    return str(uuid.uuid5(NAMESPACE, key))

def generate_scene_uuid_from_metadata(scene_metadata: Dict[str, Any]) -> str:
    return deterministic_uuid(
        scene_metadata.get("book_code", "nothing"),
        scene_metadata.get("part", "1"),
        scene_metadata.get("episode", "1"),
        scene_metadata.get("scene", "1"),
        "scene"
    )

def generate_core_identifier(scene_metadata: Dict[str, Any]) -> str:
    return f"{scene_metadata.get('book_code','nothing')}_P{scene_metadata.get('part','1')}_E{scene_metadata.get('episode','1')}_S{scene_metadata.get('scene','1')}"

# -----------------------
# Canonical Validator & Passfile I/O (unchanged)
# -----------------------
# [omitted here for brevity; same as v5.8]

# -----------------------
# Micro-Beat & Arc Utilities
# -----------------------
def assign_micro_beat_uuids(beats: List[Dict[str, Any]], scene_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    for i, b in enumerate(beats):
        if "micro_beat_uuid" not in b:
            b["micro_beat_uuid"] = deterministic_uuid(
                scene_metadata.get("book_code",""),
                scene_metadata.get("part","1"),
                scene_metadata.get("episode","1"),
                scene_metadata.get("scene","1"),
                object_type=b.get("type","micro_beat"),
                array_index=i
            )
    return beats

def compute_arcs(beats: List[Dict[str, Any]],
                 thresholds: Optional[Dict[str,float]] = None,
                 normalize_across: str = "chunk") -> List[Dict[str, Any]]:
    thresholds = thresholds or {"dominance":0.5, "emotion":0.5, "erotic":0.5}
    n = max(1, len(beats))
    
    total_dom = sum(sum(1 for w in TRINITY_TOKENS["pearls"] if w in (b.get("snippet") or "").lower()) for b in beats)
    total_emo = sum(sum(1 for w in TRINITY_TOKENS["moan"] if w in (b.get("snippet") or "").lower()) for b in beats)
    total_erot = sum(sum(1 for w in EROTIC_PHYSIOLOGY if w in (b.get("snippet") or "").lower()) for b in beats)
    
    arcs = []
    for i, b in enumerate(beats):
        dom_count = sum(1 for w in TRINITY_TOKENS["pearls"] if w in (b.get("snippet") or "").lower())
        emo_count = sum(1 for w in TRINITY_TOKENS["moan"] if w in (b.get("snippet") or "").lower())
        erot_count = sum(1 for w in EROTIC_PHYSIOLOGY if w in (b.get("snippet") or "").lower())
        
        if normalize_across == "rolling":
            dom_norm = dom_count / max(1, total_dom)
            emo_norm = emo_count / max(1, total_emo)
            erot_norm = erot_count / max(1, total_erot)
        else:
            dom_norm = dom_count / n
            emo_norm = emo_count / n
            erot_norm = erot_count / n
        
        arcs.append({
            "micro_beat_index": i,
            "dominance_norm": dom_norm,
            "emotion_norm": emo_norm,
            "erotic_norm": erot_norm,
            "dominance_label": "high" if dom_norm > thresholds["dominance"] else "low",
            "emotion_label": "high" if emo_norm > thresholds["emotion"] else "low",
            "erotic_label": "high" if erot_norm > thresholds["erotic"] else "low"
        })
    return arcs

def enforce_continuity(beats: List[Dict[str, Any]],
                       previous_scene_beats: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Assign micro_beat_index with cross-scene continuity.
    Update sections.connected_completion_arcs per beat.
    """
    start_index = 0
    if previous_scene_beats:
        last_prev_index = max(b.get("micro_beat_index", -1) for b in previous_scene_beats)
        start_index = last_prev_index + 1
    
    for i, b in enumerate(beats):
        b["micro_beat_index"] = start_index + i
        sections = b.setdefault("sections", {})
        # Simple continuity: mark connected_completion_arcs True if previous_scene_beats exist
        sections["connected_completion_arcs"] = True if previous_scene_beats else False
    
    return beats

# -----------------------
# Trinity Advisory (unchanged)
# -----------------------
def detect_trinity_cues(scene_text: str, scene_record: dict) -> dict:
    def find_tokens(text, keywords): return [k for k in keywords if re.search(rf"\b{re.escape(k)}\b", (text or "").lower())]
    pearls = find_tokens(scene_text, TRINITY_TOKENS["pearls"])
    cuffs = find_tokens(scene_text, TRINITY_TOKENS["cuffs"])
    moan = find_tokens(scene_text, TRINITY_TOKENS["moan"])
    sexact = find_tokens(scene_text, SEXUAL_ACTION_KEYWORDS)
    erophys = find_tokens(scene_text, EROTIC_PHYSIOLOGY)
    cues = sum([len(moan)>0, len(sexact)>0, len(erophys)>0, any(f in scene_record.get("scene_metadata",{}).get("flags",[]) for f in ["climax","kink","part_end","finale"])])
    return {"pearls":pearls,"cuffs":cuffs,"moan":moan,"sexact":sexact,"erophys":erophys,"cues":cues}

def insert_trinity_advisory(scene_record: Dict[str, Any]) -> Dict[str, Any]:
    cues = detect_trinity_cues(scene_record.get("scene_text",""), scene_record)
    advisory = {
        "pearls_detected": cues["pearls"],
        "cuffs_detected": cues["cuffs"],
        "moan_detected": cues["moan"],
        "sexual_actions": cues["sexact"],
        "erotic_physiology": cues["erophys"],
        "two_condition_rule_triggered": cues["cues"] >= 2,
        "advisory_strength": cues["cues"] / 4
    }
    scene_record.setdefault("sections", {})["trinity_advisory"] = advisory
    scene_record.setdefault("refs", {}).setdefault("insert_advisory_refs", []).append(scene_record.get("scene_uuid"))
    return scene_record
