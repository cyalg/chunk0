import random

def generate_dummy_chunks(num_chunks: int = 14):
    """
    Generate dummy scene chunks for testing merge and pipeline utilities.
    Each chunk includes scene_metadata, scene_text with Trinity keywords, and beats.
    """
    TRINITY_KEYWORDS = {
        "pearls": ["pearl", "necklace", "jewel", "collar", "bead"],
        "cuffs": ["cuff", "leather", "bound", "tie", "wrist"],
        "moan": ["moan", "gasp", "sigh", "pant", "whimper"]
    }

    chunks = []
    for i in range(1, num_chunks + 1):
        part = str((i - 1) // 3 + 1)        # Example: 4 parts
        episode = str((i - 1) % 3 + 1)      # Example: 3 episodes per part
        scene = str(i)
        
        # Create 2-3 micro-beats per chunk with random Trinity keywords
        beats = []
        for j in range(1, random.randint(2, 4)):
            keyword_type = random.choice(list(TRINITY_KEYWORDS.keys()))
            keyword = random.choice(TRINITY_KEYWORDS[keyword_type])
            snippet = f"Micro-beat {i}-{j}: {keyword} appears and triggers action."
            beats.append({"snippet": snippet})

        # Scene text concatenates beat snippets
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

# Example usage
if __name__ == "__main__":
    dummy_chunks = generate_dummy_chunks()
    import json
    print(json.dumps(dummy_chunks, indent=2))
