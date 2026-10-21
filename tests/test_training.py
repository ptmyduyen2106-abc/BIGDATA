"""Pure quality-contract tests plus opt-in real Spark MLlib ALS execution."""
from __future__ import annotations

import math
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest

from pipeline.ranking import (chronological_boundaries, filter_seen_ranked, ndcg_at_k,
                              precision_at_k, recall_at_k, rolling_folds, split_label,
                              user_ranking_metrics)
from pipeline.train import _export_seen_sqlite, _selection_grid


ROOT = Path(__file__).resolve().parents[1]


def temporary_directory():
    scratch = ROOT / "outputs" / "test-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=scratch)


class RankingTests(unittest.TestCase):
    def test_known_binary_ranking_values_and_fixed_precision_denominator(self):
        actual = [10, 30]
        predicted = [10, 20, 30]
        self.assertAlmostEqual(precision_at_k(predicted, actual, 3), 2 / 3)
        self.assertEqual(recall_at_k(predicted, actual, 3), 1.0)
        self.assertAlmostEqual(ndcg_at_k(predicted, actual, 3), 1.5 / (1 + 1 / math.log2(3)))
        self.assertEqual(precision_at_k([10], actual, 10), 0.1)

    def test_missing_cold_predictions_are_misses_and_duplicate_ids_do_not_inflate(self):
        self.assertEqual(user_ranking_metrics([], [10], 5),
                         {"precision_at_k": 0.0, "recall_at_k": 0.0, "ndcg_at_k": 0.0})
        self.assertEqual(precision_at_k([10, 10, 10], [10], 3), 1 / 3)
        self.assertEqual(recall_at_k([10, 10], [10, 10], 3), 1.0)
        self.assertEqual(ndcg_at_k([], [], 3), 0.0)
        with self.assertRaises(ValueError):
            precision_at_k([], [], 0)

    def test_seen_filter_is_unique_score_ordered_and_deterministic(self):
        candidates = [{"itemid": 10, "score": 9}, {"itemid": 20, "score": 3},
                      {"itemid": 30, "score": 3}, {"itemid": 20, "score": 2}]
        self.assertEqual(filter_seen_ranked(candidates, [10], 3),
                         [{"itemid": 20, "score": 3.0}, {"itemid": 30, "score": 3.0}])
        with self.assertRaises(ValueError):
            filter_seen_ranked([{"itemid": 1, "score": float("nan")}], [], 1)

    def test_chronological_splits_precede_validation_and_keep_equal_times_together(self):
        times = [value for value in range(1, 21) for repeat in range(2)]
        boundaries = chronological_boundaries(reversed(times))
        self.assertEqual(boundaries, (10, 14, 17))
        labels = {timestamp: split_label(timestamp, boundaries[1], boundaries[2]) for timestamp in times}
        self.assertEqual(labels[14], "train")
        self.assertEqual(labels[15], "validation")
        self.assertEqual(labels[18], "test")
        for fold in rolling_folds(boundaries):
            training = [timestamp for timestamp in times if timestamp <= fold["train_end_inclusive"]]
            validation = [timestamp for timestamp in times if fold["validation_start_exclusive"] < timestamp <= fold["validation_end_inclusive"]]
            self.assertLess(max(training), min(validation))
            self.assertLessEqual(max(validation), boundaries[-1])
        with self.assertRaises(ValueError):
            chronological_boundaries([1] * 20)

    def test_quick_grid_really_selects_multiple_candidates_and_full_grid_tunes_alpha(self):
        self.assertGreaterEqual(len(_selection_grid(True)), 2)
        full = _selection_grid(False)
        self.assertGreater(len({grid["rank"] for grid in full}), 1)
        self.assertGreater(len({grid["reg_param"] for grid in full}), 1)
        self.assertGreater(len({grid["alpha"] for grid in full}), 1)

    def test_sqlite_sidecar_contains_all_seen_users_beyond_json_scope(self):
        class Pairs:
            def toLocalIterator(self):
                return iter([SimpleNamespace(visitorid=1, itemid=10), SimpleNamespace(visitorid=900000, itemid=20)])
        with temporary_directory() as directory:
            destination = Path(directory) / "seen_items.sqlite"
            self.assertEqual(_export_seen_sqlite(Pairs(), destination), 2)
            with closing(sqlite3.connect(destination)) as connection:
                self.assertEqual(connection.execute("SELECT itemid FROM seen_items WHERE visitorid=?", (900000,)).fetchall(), [(20,)])
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])


@unittest.skipUnless(os.getenv("BIGDATA_RUN_SPARK_ML_TESTS") == "1", "Enable BIGDATA_RUN_SPARK_ML_TESTS=1 in the Spark runtime")
class SparkALSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("BigData-ALS-Contract-Test")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_real_als_pipeline_persists_and_filters_seen_items(self):
        from pyspark.ml import PipelineModel
        from pipeline.train import prepare_interactions, fit_als_pipeline, recommend_unseen, evaluate_ranking
        rows = [(user, item, "view") for user in range(1, 7) for item in range(1, 7)
                if (user + item) % 3 == 0]
        events = self.spark.createDataFrame(rows, "visitorid long,itemid long,event string")
        interactions = prepare_interactions(events).cache()
        model = fit_als_pipeline(interactions, {"rank": 2, "reg_param": 0.1, "alpha": 10}, max_iter=2)
        self.assertEqual(model.stages[0].getOutputCol(), "interaction_features")
        als = model.stages[-1]
        self.assertEqual(als.rank, 2)
        result = recommend_unseen(als, interactions.select("visitorid"), interactions, 2, 6).collect()
        seen = {(int(row.visitorid), int(row.itemid)) for row in interactions.collect()}
        self.assertTrue(result)
        self.assertTrue(all((int(row.visitorid), int(row.itemid)) not in seen and math.isfinite(row.score) for row in result))
        holdout = self.spark.createDataFrame([(1, 1, "view"), (999, 1, "view"), (2, 999, "view")],
                                             "visitorid long,itemid long,event string")
        metrics = evaluate_ranking(als, interactions, holdout, top_k=2)
        self.assertEqual(metrics["eligible_users"], 3)
        self.assertEqual(metrics["cold_users"], 1)
        self.assertEqual(metrics["cold_item_pairs"], 1)
        self.assertTrue(all(0 <= metrics[key] <= 1 for key in ("precision_at_k", "recall_at_k", "ndcg_at_k", "catalog_coverage", "user_coverage")))
        with temporary_directory() as directory:
            path = str(Path(directory) / "pipeline_model")
            model.write().save(path)
            loaded = PipelineModel.load(path)
            self.assertEqual(loaded.stages[-1].rank, 2)
            self.assertEqual(loaded.stages[-1].userFactors.count(), 6)
        als.userFactors.unpersist()
        als.itemFactors.unpersist()
        interactions.unpersist()


if __name__ == "__main__":
    unittest.main()
