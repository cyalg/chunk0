# -----------------------
# workflow_utils v5.5 - Full Production
# Added: Multi-chunk support in validate_minimal_canonical
# -----------------------
import re, uuid, json, logging
from pathlib import Path
from copy import deepcopy
from typing import List, Dict, Any, Optional, Union
from jsonschema import validate as jsonschema_validate, ValidationError

# -----------------------
# Logging Config
# -----------------------
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
# Utilities
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
# Canonical Validator (v5.5) - Multi-chunk Support
# -----------------------
def validate_minimal_canonical(scene_records: Union[Dict[str, Any], List[Dict[str, Any]]],
                               schema_path: Optional[Path] = SCHEMA_PATH,
                               merge: bool = False) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    Validate single or multiple scenes against canonical schema.
    - merge=True will return a dict keyed by scene_uuid.
    - Duplicates detected will log warnings and be skipped in merge.
    """
    single_input = isinstance(scene_records, dict)
    records = [scene_records] if single_input else scene_records
    validated = {}
    
    seen_uuids = set()
    
    if schema_path and schema_path.exists():
        with open(schema_path) as f:
            schema = json.load(f)
    else:
        schema = None

    for rec in records:
        rec_copy = deepcopy(rec)
        metadata = rec_copy.get("scene_metadata", {})
        rec_copy.setdefault("scene_uuid", generate_scene_uuid_from_metadata(metadata))
        rec_copy.setdefault("core_identifier", metadata.get("core_identifier", "unknown_core"))
        rec_copy.setdefault("refs", {"flag_refs": [], "insert_advisory_refs": [], "feedback_summary_ref": []})
        rec_copy.setdefault("folder_map", {})

        if rec_copy["scene_uuid"] in seen_uuids:
            logging.warning(f"Duplicate scene_uuid detected: {rec_copy['scene_uuid']}. Skipping duplicate.")
            continue
        seen_uuids.add(rec_copy["scene_uuid"])

        if schema:
            try:
                jsonschema_validate(instance=rec_copy, schema=schema)
            except ValidationError as e:
                logging.error(f"Schema validation failed for scene_uuid {rec_copy['scene_uuid']}: {e}")
                raise

        validated[rec_copy["scene_uuid"]] = rec_copy

    if single_input:
        return next(iter(validated.values()), {})
    return validated if merge else list(validated.values())

# -----------------------
# Passfile I/O
# -----------------------
def read_passfile(path: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path) if path else PASSFILE_PATH
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def write_passfile(data: Dict[str, Any], path: Optional[str] = None, overwrite: bool = True) -> None:
    p = Path(path) if path else PASSFILE_PATH
    if p.exists() and not overwrite:
        backup_path = p.with_suffix(".bak")
        p.rename(backup_path)
        logging.info(f"Existing passfile backed up to {backup_path}")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def write_passfile_strict(key: str, data: Any, path: Optional[str] = None, overwrite: bool = True) -> None:
    pf = read_passfile(path)
    pf[key] = data
    write_passfile(pf, path, overwrite=overwrite)

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

def compute_arcs(beats: List[Dict[str, Any]], thresholds: Optional[Dict[str,float]] = None) -> List[Dict[str, Any]]:
    thresholds = thresholds or {"dominance":0.5, "emotion":0.5, "erotic":0.5}
    n = max(1, len(beats))
    arcs = []
    for i, b in enumerate(beats):
        dom = sum(1 for w in TRINITY_TOKENS["pearls"] if w in (b.get("snippet") or "").lower())
        emo = sum(1 for w in TRINITY_TOKENS["moan"] if w in (b.get("snippet") or "").lower())
        erot = sum(1 for w in EROTIC_PHYSIOLOGY if w in (b.get("snippet") or "").lower())
        arcs.append({
            "micro_beat_index": i,
            "dominance_norm": dom / n,
            "emotion_norm": emo / n,
            "erotic_norm": erot / n,
            "dominance_label": "high" if dom/n > thresholds["dominance"] else "low",
            "emotion_label": "high" if emo/n > thresholds["emotion"] else "low",
            "erotic_label": "high" if erot/n > thresholds["erotic"] else "low"
        })
    return arcs

def enforce_continuity(beats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for i, b in enumerate(beats):
        b["micro_beat_index"] = i
    return beats

# -----------------------
# Trinity Advisory
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
