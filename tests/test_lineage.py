"""Regressions for preserving live ingestion through model replacements."""
import tempfile
from pathlib import Path
import unittest

from pipeline.config import atomic_json, resolve_data_run_id, run_path


class DataLineageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)

    def report(self, run, **fields):
        atomic_json(run_path(run, self.root) / 'metrics/run_report.json', {'run_id': run, **fields})

    def test_ingestion_run_and_legacy_missing_report(self):
        self.assertEqual(resolve_data_run_id('live-01', self.root), 'live-01')
        self.report('live-01', source_run_id=None)
        self.assertEqual(resolve_data_run_id('live-01', self.root), 'live-01')

    def test_multiple_model_generations_keep_live_data_source(self):
        self.report('model-2', source_run_id='model-1')
        self.report('model-1', source_run_id='live-01')
        self.report('live-01', source_run_id=None)
        source = resolve_data_run_id('model-2', self.root)
        self.assertEqual(source, 'live-01')
        # Data arriving after model-1 was trained remains in the selected path.
        clean = run_path(source, self.root) / 'clean'
        clean.mkdir()
        (clean / 'newly-committed-event.txt').write_text('actual future event')
        self.assertTrue((run_path(resolve_data_run_id('model-2', self.root), self.root)
                         / 'clean/newly-committed-event.txt').exists())

    def test_explicit_canonical_data_source(self):
        self.report('model-2', source_run_id='model-1', source_data_run_id='live-01')
        self.assertEqual(resolve_data_run_id('model-2', self.root), 'live-01')

    def test_cycle_is_rejected(self):
        self.report('model-2', source_run_id='model-1')
        self.report('model-1', source_run_id='model-2')
        with self.assertRaisesRegex(ValueError, 'Cycle'):
            resolve_data_run_id('model-2', self.root)

    def test_malformed_source_and_path_escape_are_rejected(self):
        for source in [42, 0, '', {}, [], '../../unrelated-run']:
            self.report('model-1', source_run_id=source)
            with self.subTest(source=source), self.assertRaises(ValueError):
                resolve_data_run_id('model-1', self.root)

    def test_identity_mismatch_is_rejected(self):
        path = run_path('model-1', self.root) / 'metrics/run_report.json'
        atomic_json(path, {'run_id': 'unrelated', 'source_run_id': None})
        with self.assertRaisesRegex(ValueError, 'identity'):
            resolve_data_run_id('model-1', self.root)


if __name__ == '__main__':
    unittest.main()
