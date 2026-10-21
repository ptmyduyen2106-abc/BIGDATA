"""External CSV sort correctness, stability and no-clobber behavior."""
import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.sort_events import FIELDS, sort_events


ROOT = Path(__file__).resolve().parents[1]


class SortEventsTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / "outputs" / "test-tmp"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.directory = Path(self.temporary.name)
        self.assertTrue(self.directory.resolve().is_relative_to(scratch.resolve()))

    def tearDown(self):
        self.temporary.cleanup()

    def write_csv(self, rows, name="input.csv", header=FIELDS):
        path = self.directory / name
        with path.open("w", newline="", encoding="utf-8") as sink:
            writer = csv.writer(sink)
            writer.writerow(header)
            writer.writerows(rows)
        return path

    def read_csv(self, path):
        with path.open(newline="", encoding="utf-8") as stream:
            return list(csv.reader(stream))

    def test_sort_across_chunks_preserves_every_raw_field(self):
        rows = [[str(timestamp), str(index), "view", "00101", ""]
                for index, timestamp in enumerate([9, 2, 7, 1, 10, 3, 8])]
        source = self.write_csv(rows)
        output = self.directory / "sorted.csv"
        report = sort_events(source, output, chunk_rows=2)
        self.assertEqual(self.read_csv(output), [list(FIELDS), *sorted(rows, key=lambda r: int(r[0]))])
        self.assertEqual(report["rows"], 7)
        self.assertEqual(report["initial_chunks"], 4)
        self.assertEqual(self.read_csv(source), [list(FIELDS), *rows])
        self.assertEqual(list(self.directory.glob(".sort-events-*")), [])

    def test_equal_timestamps_stable_across_multiple_merge_passes(self):
        rows = [[str((index % 3) + 1), str(index), "view", "1", ""] for index in range(140)]
        source = self.write_csv(rows)
        output = self.directory / "sorted.csv"
        report = sort_events(source, output, chunk_rows=1)
        self.assertGreaterEqual(report["merge_passes"], 2)
        self.assertEqual(self.read_csv(output)[1:], sorted(rows, key=lambda r: int(r[0])))

    def test_invalid_timestamp_or_header_leaves_no_output(self):
        for bad_timestamp in ("1.5", "", "not-a-time", "-1", "0", " 2"):
            source = self.write_csv([[bad_timestamp, "1", "view", "2", ""]])
            output = self.directory / "sorted.csv"
            with self.subTest(timestamp=bad_timestamp), self.assertRaises(ValueError):
                sort_events(source, output, chunk_rows=1)
            self.assertFalse(output.exists())
        source = self.write_csv([], header=("timestamp", "visitorid", "event", "itemid", "extra"))
        with self.assertRaises(ValueError):
            sort_events(source, self.directory / "sorted.csv")

    def test_existing_output_and_input_overwrite_are_rejected(self):
        source = self.write_csv([["1", "1", "view", "2", ""]])
        original = source.read_bytes()
        with self.assertRaises(ValueError):
            sort_events(source, source)
        output = self.directory / "sorted.csv"
        output.write_text("keep me", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            sort_events(source, output)
        self.assertEqual(source.read_bytes(), original)
        self.assertEqual(output.read_text(encoding="utf-8"), "keep me")

    def test_atomic_commit_race_does_not_clobber_competing_file(self):
        source = self.write_csv([["2", "1", "view", "2", ""]])
        output = self.directory / "sorted.csv"
        from scripts.sort_events import _publish_no_clobber
        def race_create(completed, destination):
            Path(destination).write_text("other writer", encoding="utf-8")
            return _publish_no_clobber(completed, destination)
        with patch("scripts.sort_events._publish_no_clobber", side_effect=race_create), self.assertRaises(FileExistsError):
            sort_events(source, output)
        self.assertEqual(output.read_text(encoding="utf-8"), "other writer")

    def test_empty_csv_with_valid_header_outputs_header_only(self):
        source = self.write_csv([])
        output = self.directory / "sorted.csv"
        report = sort_events(source, output)
        self.assertEqual(report["rows"], 0)
        self.assertEqual(self.read_csv(output), [list(FIELDS)])


if __name__ == "__main__":
    unittest.main()
