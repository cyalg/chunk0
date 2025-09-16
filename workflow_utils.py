# -----------------------
# workflow_utils v5.11 - Full Production
# Added: standardized logging & exception handling
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
# Canonical Validator & Passfile I/O
# -----------------------
def validate_minimal_canonical(scene_records: Union[Dict[str, Any], List[Dict[str, Any]]],
                               schema_path: Optional[Path] = SCHEMA_PATH,
                               merge: bool = False) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    single_input = isinstance(scene_records, dict)
    records = [scene_records] if single_input else scene_records
    validated = {}
    seen_uuids = set()

    schema = None
    if schema_path and schema_path.exists():
        try:
            with open(schema_path) as f:
                schema = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load schema: {e}")
            raise

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
                continue  # Skip invalid record instead of raising

        validated[rec_copy["scene_uuid"]] = rec_copy

    return next(iter(validated.values()), {}) if single_input else (validated if merge else list(validated.values()))

def read_passfile(path: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path) if path else PASSFILE_PATH
    try:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Failed to read passfile {p}: {e}")
    return {}

def write_passfile(data: Dict[str, Any], path: Optional[str] = None, overwrite: bool = True) -> None:
    p = Path(path) if path else PASSFILE_PATH
    tmp_file = None
    try:
        if p.exists() and not overwrite:
            backup_path = p.with_suffix(".bak")
            p.rename(backup_path)
            logging.info(f"Existing passfile backed up to {backup_path}")

        tmp_file = tempfile.NamedTemporaryFile("w", delete=False, dir=p.parent, encoding="utf-8")
        json.dump(data, tmp_file, ensure_ascii=False, indent=2)
        tmp_file.close()
        shutil.move(tmp_file.name, p)
        logging.info(f"Passfile written successfully to {p}")
    except Exception as e:
        logging.error(f"Failed to write passfile {p}: {e}")
        if tmp_file:
            tmp_file.close()
            Path(tmp_file.name).unlink(missing_ok=True)
        raise

def write_passfile_strict(key: str, data: Any, path: Optional[str] = None, overwrite: bool = True) -> None:
    try:
        pf = read_passfile(path)
        pf[key] = data
        write_passfile(pf, path, overwrite=overwrite)
    except Exception as e:
        logging.error(f"Failed to write key '{key}' to passfile: {e}")
        raise

def merge_passfile_chunks(chunks: List[Dict[str, Any]],
                          path: Optional[str] = None,
                          overwrite_existing: bool = False) -> None:
    pf_path = Path(path) if path else PASSFILE_PATH
    passfile_data = read_passfile(pf_path)
    try:
        validated_chunks = validate_minimal_canonical(chunks, merge=True)
    except Exception as e:
        logging.error(f"Validation failed during merge: {e}")
        return

    for scene_uuid, chunk in validated_chunks.items():
        if scene_uuid in passfile_data:
            if overwrite_existing:
                logging.info(f"Overwriting existing scene_uuid {scene_uuid}.")
            else:
                logging.warning(f"Scene UUID {scene_uuid} exists. Skipping merge.")
                continue
        passfile_data[scene_uuid] = chunk

    try:
        write_passfile(passfile_data, pf_path, overwrite=True)
    except Exception as e:
        logging.error(f"Failed to write merged passfile: {e}")
        raise

# -----------------------
# Micro-Beat, Arc & Continuity Utilities
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
    start_index = 0
    if previous_scene_beats:
        last_prev_index = max(b.get("micro_beat_index", -1) for b in previous_scene_beats)
        start_index = last_prev_index + 1
    
    for i, b in enumerate(beats):
        b["micro_beat_index"] = start_index + i
        sections = b.setdefault("sections", {})
        sections["connected_completion_arcs"] = bool(previous_scene_beats)
    
    return beats

# -----------------------
# Trinity Advisory
# -----------------------
def detect_trinity_cues(scene_text: str, scene_record: dict) -> dict:
    def find_tokens(text, keywords): 
        return [k for k in keywords if re.search(rf"\b{re.escape(k)}\b", (text or "").lower())]
    pearls = find_tokens(scene_text, TRINITY_TOKENS["pearls"])
    cuffs = find_tokens(scene_text, TRINITY_TOKENS["cuffs"])
    moan = find_tokens(scene_text, TRINITY_TOKENS["moan"])
    sexact = find_tokens(scene_text, SEXUAL_ACTION_KEYWORDS)
    erophys = find_tokens(scene_text, EROTIC_PHYSIOLOGY)
    cues = sum([len(moan)>0, len(sexact)>0, len(erophys)>0, any(f in scene_record.get("scene_metadata",{}).get("flags",[]) for f in ["climax","kink","part_end","finale"])])
    return {"pearls":pearls,"cuffs":cuffs,"moan":moan,"sexact":sexact,"erophys":erophys,"cues":cues}

def insert_trinity_advisory(scene_record: Dict[str, Any]) -> Dict[str, Any]:
    cues = detect_trinity_cues(scene_record.get("scene_text",""), scene_record)
    sections = scene_record.setdefault("sections", {})
    refs = scene_record.setdefault("refs", {})

    existing = sections.get("trinity_advisory", {})

    def merge_lists(old, new):
        return list(set(old or []) | set(new or []))

    advisory = {
        "pearls_detected": merge_lists(existing.get("pearls_detected"), cues["pearls"]),
        "cuffs_detected": merge_lists(existing.get("cuffs_detected"), cues["cuffs"]),
        "moan_detected": merge_lists(existing.get("moan_detected"), cues["moan"]),
        "sexual_actions": merge_lists(existing.get("sexual_actions"), cues["sexact"]),
        "erotic_physiology": merge_lists(existing.get("erotic_physiology"), cues["erophys"])
    }

    total_cues = sum(bool(advisory[key]) for key in ["moan_detected","sexual_actions","erotic_physiology"])
    scene_flags = scene_record.get("scene_metadata", {}).get("flags", [])
    total_cues += sum(f in ["climax","kink","part_end","finale"] for f in scene_flags)

    advisory["two_condition_rule_triggered"] = total_cues >= 2
    advisory["advisory_strength"] = total_cues / 4

    sections["trinity_advisory"] = advisory

    refs.setdefault("insert_advisory_refs", [])
    scene_uuid = scene_record.get("scene_uuid")
    if scene_uuid and scene_uuid not in refs["insert_advisory_refs"]:
        refs["insert_advisory_refs"].append(scene_uuid)

    return scene_record
