import unittest
from copy import deepcopy
from workflow_utils import (
    deterministic_uuid,
    validate_minimal_canonical,
    merge_passfile_chunks,
    assign_micro_beat_uuids,
    enforce_continuity,
    insert_trinity_advisory,
)

class TestWorkflowUtils(unittest.TestCase):

    def setUp(self):
        # Sample scene metadata
        self.scene_metadata = {
            "book_code": "TESTBOOK",
            "part": "1",
            "episode": "1",
            "scene": "1",
            "scene_flags": [],
            "allowed_awakenings_per_part": 2,
            "trinity_insertion_budget": 3,
            "sexualization_level": 2
        }

        # Sample scene record
        self.scene_record = {
            "scene_metadata": deepcopy(self.scene_metadata),
            "scene_text": "She wore a pearl necklace and let out a soft moan while restrained by leather cuffs."
        }

        # Sample beats
        self.beats = [
            {"snippet": "pearl glinting on her collar", "type": "micro_beat"},
            {"snippet": "soft gasp from the wrist-bound hand", "type": "micro_beat"},
            {"snippet": "pulse quickens", "type": "micro_beat"}
        ]

    # -----------------------
    # deterministic_uuid
    # -----------------------
    def test_deterministic_uuid_repeatability(self):
        uuid1 = deterministic_uuid("BOOK", "1", "1", "1", "scene")
        uuid2 = deterministic_uuid("BOOK", "1", "1", "1", "scene")
        self.assertEqual(uuid1, uuid2)

    def test_deterministic_uuid_uniqueness(self):
        uuid1 = deterministic_uuid("BOOK", "1", "1", "1", "scene")
        uuid2 = deterministic_uuid("BOOK", "1", "1", "2", "scene")
        self.assertNotEqual(uuid1, uuid2)

    # -----------------------
    # validate_minimal_canonical
    # -----------------------
    def test_validate_minimal_canonical_valid(self):
        validated = validate_minimal_canonical([self.scene_record], merge=True)
        self.assertIn(self.scene_record["scene_metadata"]["scene"], [v["scene_metadata"]["scene"] for v in validated.values()])

    # -----------------------
    # merge_passfile_chunks
    # -----------------------
    def test_merge_passfile_chunks_duplicate_behavior(self):
        import tempfile, json
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.close()
        chunk1 = deepcopy(self.scene_record)
        chunk2 = deepcopy(self.scene_record)  # duplicate
        merge_passfile_chunks([chunk1, chunk2], path=tmpfile.name, overwrite_existing=False)
        with open(tmpfile.name) as f:
            data = json.load(f)
        self.assertEqual(len(data), 1)

    # -----------------------
    # assign_micro_beat_uuids
    # -----------------------
    def test_assign_micro_beat_uuids(self):
        beats_with_uuids = assign_micro_beat_uuids(deepcopy(self.beats), self.scene_metadata)
        uuids = [b["micro_beat_uuid"] for b in beats_with_uuids]
        self.assertEqual(len(set(uuids)), len(uuids))  # all unique

    # -----------------------
    # enforce_continuity
    # -----------------------
    def test_enforce_continuity_across_scenes(self):
        prev_beats = assign_micro_beat_uuids(deepcopy(self.beats), self.scene_metadata)
        next_beats = assign_micro_beat_uuids([{"snippet": "new beat"}], self.scene_metadata)
        continuous_beats = enforce_continuity(next_beats, prev_beats)
        self.assertEqual(continuous_beats[0]["micro_beat_index"], max(b["micro_beat_index"] for b in prev_beats)+1)

    # -----------------------
    # insert_trinity_advisory
    # -----------------------
    def test_insert_trinity_advisory_detection_and_merging(self):
        record = insert_trinity_advisory(deepcopy(self.scene_record))
        advisory = record["sections"]["trinity_advisory"]
        self.assertTrue(advisory["two_condition_rule_triggered"])
        self.assertIn("pearl", advisory["pearls_detected"])
        self.assertIn("cuff", advisory["cuffs_detected"])
        self.assertIn("moan", advisory["moan_detected"])

if __name__ == "__main__":
    unittest.main()
