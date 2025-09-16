from tests.generate_dummy_chunks import generate_chunks_from_text
import json
from pathlib import Path

sample_text = """Sophie Vert’s Office, 56th floor of East Midtown, Manhattan
A soft knock echoed, the faint hum of the air conditioner mingling with the distant clatter of heels on marble.
- “Come!” Sophie invites the person in, ... (rest of your text)"""

chunks = generate_chunks_from_text(sample_text)

out_path = Path("data/passfile.json")
out_path.parent.mkdir(exist_ok=True)
with open(out_path, "w") as f:
    json.dump({"chunks": chunks}, f, indent=2)

print(f"Saved {len(chunks)} chunks to {out_path}")
