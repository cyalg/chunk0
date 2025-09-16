import uuid
import random
from hashlib import sha1

# -----------------------
# Deterministic UUID helper
# -----------------------
def deterministic_uuid(*args) -> str:
    """Generate a UUID based on a deterministic hash of input args."""
    h = sha1("::".join(map(str, args)).encode("utf-8")).hexdigest()
    return str(uuid.UUID(h[:32]))

# -----------------------
# Trinity keywords for micro-beats
# -----------------------
TRINITY_KEYWORDS = {
    "pearls": ["pearl", "necklace", "jewel", "collar", "bead"],
    "cuffs": ["cuff", "leather", "bound", "tie", "wrist"],
    "moan": ["moan", "gasp", "sigh", "pant", "whimper"]
}

# -----------------------
# Split text into chunks
# -----------------------
def split_text_to_chunks(text: str, num_chunks: int = 14) -> list[str]:
    words = text.split()
    chunk_size = max(1, len(words) // num_chunks)
    return [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)][:num_chunks]

# -----------------------
# Generate beats for a chunk
# -----------------------
def generate_beats(chunk_text: str, scene_metadata: dict) -> list[dict]:
    words = chunk_text.split()
    beats = []
    for i in range(0, len(words), max(5, len(words)//3)):
        snippet_words = words[i:i+5]
        snippet = " ".join(snippet_words)
        # Randomly attach a Trinity keyword
        kw_type = random.choice(list(TRINITY_KEYWORDS.keys()))
        kw = random.choice(TRINITY_KEYWORDS[kw_type])
        snippet += f" [{kw}]"
        beat_uuid = deterministic_uuid(scene_metadata["book_code"], scene_metadata["part"],
                                       scene_metadata["episode"], scene_metadata["scene"], i)
        beats.append({"snippet": snippet, "beat_uuid": beat_uuid})
    return beats

# -----------------------
# Generate Chunk Dicts
# -----------------------
def generate_chunks_from_text(text: str, book_code="TEST", part="1", episode="1") -> list[dict]:
    chunk_texts = split_text_to_chunks(text, num_chunks=14)
    chunks = []
    for idx, chunk_text in enumerate(chunk_texts, start=1):
        scene = str(idx)
        scene_metadata = {
            "book_code": book_code,
            "part": part,
            "episode": episode,
            "scene": scene,
            "scene_uuid": deterministic_uuid(book_code, part, episode, scene),
            "scene_title": f"Scene {scene}",
            "concise_summary": f"Auto-generated summary for scene {scene}",
            "merch_refs": [],
            "flags": [],
            "previous_scene": str(idx-1) if idx > 1 else None,
            "next_scene": str(idx+1) if idx < 14 else None
        }
        beats = generate_beats(chunk_text, scene_metadata)
        chunk = {
            "scene_metadata": scene_metadata,
            "scene_text": chunk_text,
            "beats": beats,
            "micro_beats": [{"beat_uuid": b["beat_uuid"], "text": b["snippet"], "keyword_counts": {k: b["snippet"].count(k) for k in TRINITY_KEYWORDS}} for b in beats],
            "sections": {
                "emotional_arc": {},
                "erotic_arc": {},
                "pacing_strategy_notes": {},
                "connected_completion_arcs": [],
                "trinity_advisory": {
                    "pearls_detected": [],
                    "cuffs_detected": [],
                    "moan_detected": [],
                    "sexual_actions": [],
                    "erotic_physiology": [],
                    "two_condition_rule_triggered": False,
                    "advisory_strength": 0
                }
            },
            "refs": {
                "scene_uuid": scene_metadata["scene_uuid"],
                "insert_advisory_refs": [],
                "flag_refs": []
            },
            "cross_references": {
                "previous_scene": scene_metadata["previous_scene"],
                "next_scene": scene_metadata["next_scene"]
            },
            "core_identifier": f"{book_code}_P{part}_E{episode}_S{scene}"
        }
        chunks.append(chunk)
    return chunks
