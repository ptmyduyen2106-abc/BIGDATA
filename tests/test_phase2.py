"""Meaningful offline checks; no Kafka broker or kafka-python is needed."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from phase2.contract import (ContractError, EVENT_TO_TOPIC, TOPICS, build_event,
                             event_id_for, serialize_event, validate_event)
from phase2.producer import main, publish, read_events
from phase2.verify import inspect_record


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "phase2" / "fixtures" / "synthetic_events.csv"


def temporary_directory():
    # Keep test writes inside the project, including in restricted desktop runtimes.
    scratch = ROOT / "outputs" / "test-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=scratch)


def row(**changes):
    value = dict(timestamp="1430622004384", visitorid="693516", event="view", itemid="297662", transactionid="")
    value.update(changes)
    return value


class ContractTests(unittest.TestCase):
    def test_all_four_topics_and_raw_names_are_preserved(self):
        for raw, topic in EVENT_TO_TOPIC.items():
            event = build_event(row(event=raw, transactionid="123" if raw == "transaction" else ""), "r")
            self.assertEqual(event["event"], raw)
            self.assertEqual(event["event_type"], topic)
            validate_event(event, topic=topic)

    def test_hash_matches_documented_canonical_five_field_array(self):
        canonical = b'[1430622004384,693516,"view",297662,null]'
        self.assertEqual(event_id_for(row()), hashlib.sha256(canonical).hexdigest())

    def test_replay_identity_independent_of_run_and_ingestion(self):
        first = build_event(row(), "first", ingested_at="2026-01-01T00:00:00+00:00")
        second = build_event(row(), "replay", ingested_at="2026-02-01T00:00:00+00:00")
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["timestamp"], 1430622004384)
        self.assertEqual(first["event_time"], "2015-05-03T03:00:04.384+00:00")

    def test_blank_transaction_normalizes_to_json_null(self):
        self.assertIsNone(build_event(row(transactionid="  "), "r")["transactionid"])
        self.assertEqual(event_id_for(row(visitorid="000693516")), event_id_for(row()))

    def test_unknown_event_and_invalid_ids_rejected(self):
        for changes in ({"event": "click"}, {"visitorid": "-1"}, {"itemid": "3.5"},
                        {"timestamp": "0"}, {"timestamp": True}, {"visitorid": 1.2},
                        {"event": "transaction", "transactionid": ""}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                build_event(row(**changes), "r")

    def test_missing_columns_and_blank_run_rejected(self):
        missing = row()
        missing.pop("transactionid")
        with self.assertRaises(ContractError):
            build_event(missing, "r")
        with self.assertRaises(ContractError):
            build_event(row(), " ")

    def test_wrong_topic_and_tampered_hash_rejected(self):
        event = build_event(row(), "r")
        with self.assertRaises(ContractError):
            validate_event(event, topic="purchase")
        event["itemid"] += 1
        with self.assertRaises(ContractError):
            validate_event(event)

    def test_wire_integer_schema_and_timezone_enforced(self):
        for changes in ({"schema_version": True}, {"visitorid": "693516"},
                        {"ingested_at": "2026-10-17T01:00:00"},
                        {"event_type": "view"}, {"event_time": "bad"}):
            event = build_event(row(), "r")
            event.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_event(event)

    def test_synthetic_review_is_explicit_and_unicode_serializes(self):
        event = build_event(row(event="review", synthetic="true", rating="5", review_text="Đánh giá thử nghiệm"), "r")
        self.assertTrue(event["synthetic"])
        self.assertEqual(event["rating"], 5)
        self.assertEqual(json.loads(serialize_event(event))["review_text"], "Đánh giá thử nghiệm")
        with self.assertRaises(ContractError):
            build_event(row(event="review", rating="6"), "r")


class ProducerTests(unittest.TestCase):
    def test_fixture_contains_eight_labeled_rows_and_four_topics(self):
        events = list(read_events(FIXTURE, "test"))
        self.assertEqual(len(events), 8)
        self.assertEqual({e["event_type"] for e in events}, set(TOPICS))
        self.assertTrue(all(e["synthetic"] is True for e in events))
        self.assertEqual(len(list(read_events(FIXTURE, "test", limit=3))), 3)

    def test_bounded_pending_and_acknowledgement_counts(self):
        live = peak = 0
        class Future:
            def get(self, timeout):
                nonlocal live
                live -= 1
        def send(topic, key, value):
            nonlocal live, peak
            live += 1
            peak = max(peak, live)
            event = json.loads(value)
            self.assertEqual(key, str(event["visitorid"]).encode())
            self.assertEqual(topic, event["event_type"])
            return Future()
        result = publish(read_events(FIXTURE, "bounded"), send, rate=0, max_pending=2)
        self.assertEqual(peak, 2)
        self.assertEqual(live, 0)
        self.assertEqual(result["sent"], 8)
        self.assertEqual(result["acknowledged"], 8)
        self.assertEqual(result["topic_counts"], {t: 2 for t in TOPICS})

    def test_ack_failure_propagates_with_partial_metrics(self):
        class Failing:
            def get(self, timeout):
                raise TimeoutError("ack did not arrive")
        progress = {}
        with self.assertRaises(TimeoutError):
            publish(read_events(FIXTURE, "failed"), lambda *args: Failing(),
                    rate=0, max_pending=1, progress=progress)
        self.assertEqual(progress["sent"], 1)
        self.assertEqual(progress["acknowledged"], 0)

    def test_timestamp_pacing_rejects_unsorted_input(self):
        events = list(read_events(FIXTURE, "test", limit=2))[::-1]
        class Future:
            def get(self, timeout):
                pass
        with self.assertRaises(ContractError):
            publish(events, lambda *args: Future(), rate=0, pace_from_timestamps=True)

    def test_dry_run_writes_jsonl_and_metrics_without_kafka(self):
        with temporary_directory() as directory:
            output = Path(directory) / "events.jsonl"
            report = Path(directory) / "producer.json"
            with contextlib.redirect_stdout(io.StringIO()):
                code = main(["--input", str(FIXTURE), "--run-id", "offline", "--dry-run",
                             "--rate", "0", "--output", str(output), "--report", str(report)])
            self.assertEqual(code, 0)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 8)
            result = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result["mode"], "dry_run")
            self.assertEqual(result["written"], 8)
            self.assertNotIn("acknowledged", result)

    def test_bad_csv_emits_failure_report_and_nonzero_exit(self):
        with temporary_directory() as directory:
            source = Path(directory) / "bad.csv"
            source.write_text("timestamp,visitorid,event,itemid,transactionid\n1,2,unknown,3,\n", encoding="utf-8")
            report = Path(directory) / "result.json"
            with contextlib.redirect_stdout(io.StringIO()):
                code = main(["--input", str(source), "--run-id", "bad", "--dry-run", "--rate", "0",
                             "--output", str(Path(directory) / "events.jsonl"), "--report", str(report)])
            self.assertEqual(code, 1)
            result = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "failed")
            self.assertIn("CSV data row 1", result["error"])

    def test_empty_input_does_not_report_success(self):
        with self.assertRaises(ContractError):
            publish([], lambda *args: None, rate=0)


class VerificationTests(unittest.TestCase):
    def make_record(self, event, **overrides):
        values = dict(topic=event["event_type"], partition=0, offset=5,
                      value=serialize_event(event), key=str(event["visitorid"]).encode())
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_verifier_checks_scope_topic_key_and_contract(self):
        event = build_event(row(), "right-run")
        record = self.make_record(event)
        self.assertEqual(inspect_record(record, "right-run"), event)
        self.assertIsNone(inspect_record(record, "other-run"))
        for overrides in ({"topic": "purchase"}, {"key": b"wrong"}, {"value": b"not JSON"}):
            with self.subTest(overrides=overrides), self.assertRaises(ContractError):
                inspect_record(self.make_record(event, **overrides), "right-run")


if __name__ == "__main__":
    unittest.main()
