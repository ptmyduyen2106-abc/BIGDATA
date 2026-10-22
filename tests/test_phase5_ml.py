"""Kiểm thử Spark thật (local) cho phase5: đáp án tính tay trên dữ liệu rất nhỏ.

Chạy:  python -I -m unittest discover -s tests -p test_phase5_ml.py -v   (cần Java + PySpark)
"""
from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession  # noqa: E402

from phase5_ml.data import build_gold, verify_gold_consistency  # noqa: E402
from phase5_ml.evaluate import novel_targets, popularity_recommendations, score_users, summarize, stratified_users  # noqa: E402
from phase5_ml.explicit import rmse_window  # noqa: E402
from pipeline.train import prepare_interactions  # noqa: E402


class Phase5Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("phase5-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self, rows):
        return self.spark.createDataFrame(rows, "timestamp long, visitorid long, event string, itemid long")

    def test_gold_matches_als_rating(self):
        ev = self.events([(1, 1, "view", 10), (2, 1, "view", 10), (3, 1, "addtocart", 10),
                          (4, 1, "transaction", 10), (5, 2, "view", 20)])
        gold = build_gold(ev)
        got = {(r.visitorid, r.itemid): (r.interaction_score, r.interaction_count) for r in gold.collect()}
        self.assertEqual(got[(1, 10)], (1 + 1 + 3 + 5, 4))
        self.assertEqual(got[(2, 20)], (1.0, 1))
        check = verify_gold_consistency(ev, gold)
        self.assertTrue(check["consistent"])
        self.assertEqual(prepare_interactions(ev).filter("visitorid=1 and itemid=10").first().rating, 10.0)

    def test_popularity_excludes_seen_and_orders_by_score(self):
        ev = self.events([(1, 1, "transaction", 10), (2, 2, "view", 10), (3, 2, "view", 20),
                          (4, 3, "view", 30), (5, 3, "view", 20), (6, 3, "view", 20)])
        history = prepare_interactions(ev).cache()
        # điểm: item10 = 5+1 = 6 ; item20 = 1+1+1 = 3 ; item30 = 1
        users = self.spark.createDataFrame([(1,), (3,)], "visitorid int")
        rows = popularity_recommendations(history, users, k=2).collect()
        by_user = {}
        for r in sorted(rows, key=lambda r: (r.visitorid, r["rank"])):
            by_user.setdefault(r.visitorid, []).append(r.itemid)
        self.assertEqual(by_user[1], [20, 30])      # user1 đã xem 10
        self.assertEqual(by_user[3], [10])          # user3 đã xem 20,30 -> chỉ còn 10

    def test_metrics_hand_computed_with_warm_cold_weighting(self):
        # user 1 (warm): mục tiêu {10, 30}; gợi ý [10, 20, 30] -> hits=2 ; K=3
        # user 2 (cold): mục tiêu {50}; không có gợi ý -> 0
        targets = self.spark.createDataFrame([(1, 10), (1, 30), (2, 50)], "visitorid int, itemid int")
        preds = self.spark.createDataFrame([(1, 10, 1.0, 1), (1, 20, 0.9, 2), (1, 30, 0.8, 3)],
                                           "visitorid int, itemid int, score double, rank int")
        scored = score_users(targets, preds, 3)
        rows = {r.visitorid: r for r in scored.collect()}
        self.assertAlmostEqual(rows[1].precision, 2 / 3)
        self.assertAlmostEqual(rows[1].recall, 1.0)
        self.assertAlmostEqual(rows[1].ndcg, (1 + 1 / math.log2(4)) / (1 + 1 / math.log2(3)))
        self.assertEqual(rows[2].precision, 0.0)
        self.assertEqual(rows[2].ndcg, 0.0)
        users = self.spark.createDataFrame([(1, True), (2, False)], "visitorid int, is_warm boolean")
        summary = summarize(scored, users, {"warm": 10, "cold": 90})
        self.assertAlmostEqual(summary["warm"]["precision_at_k"], 2 / 3)
        self.assertEqual(summary["cold"]["precision_at_k"], 0.0)
        # gia quyền theo quần thể: (10 * 2/3 + 90 * 0) / 100
        self.assertAlmostEqual(summary["overall_weighted"]["precision_at_k"], 10 * (2 / 3) / 100)

    def test_novel_targets_drop_pairs_seen_in_train(self):
        train = self.events([(1, 1, "view", 10), (2, 2, "view", 20)])
        history = prepare_interactions(train)
        window = self.events([(3, 1, "view", 10), (4, 1, "view", 11), (5, 3, "view", 30)])
        pairs = sorted((r.visitorid, r.itemid) for r in novel_targets(history, window).collect())
        self.assertEqual(pairs, [(1, 11), (3, 30)])

    def test_stratified_sampling_is_deterministic_and_respects_limits(self):
        eligible = self.spark.createDataFrame([(i,) for i in range(1, 21)], "visitorid int")
        warm = self.spark.createDataFrame([(i,) for i in range(1, 11)], "visitorid int")
        first, pop = stratified_users(eligible, warm, 3, 2, seed=7)
        again, _ = stratified_users(eligible, warm, 3, 2, seed=7)
        a = sorted((r.visitorid, r.is_warm) for r in first.collect())
        b = sorted((r.visitorid, r.is_warm) for r in again.collect())
        self.assertEqual(a, b)
        self.assertEqual(sum(1 for _, w in a if w), 3)
        self.assertEqual(sum(1 for _, w in a if not w), 2)
        self.assertEqual(pop, {"warm": 10, "cold": 10})

    def test_rmse_window_baselines_and_novelty(self):
        rows, t = [], 1
        for user in range(1, 21):
            for item in range(1, 11):
                if (user, item) == (5, 7):      # cặp này vắng trong train để test có cặp MỚI hợp lệ
                    continue
                rows.append((t, user, "view" if (user + item) % 3 else "addtocart", item))
                t += 1
        train = self.events(rows)
        window = self.events([(1000, 5, "transaction", 7),    # cặp mới, user & item đã biết -> được chấm (level 5)
                              (1001, 2, "transaction", 11),   # item chưa biết -> cold, bị drop
                              (1002, 3, "view", 5)])           # cặp đã có trong train -> loại khỏi mục tiêu
        result = rmse_window(train, window, rank=2, reg_param=0.1, max_iter=5)
        self.assertEqual(result["status"], "evaluated")
        self.assertEqual(result["novel_test_pairs"], 2)
        self.assertEqual(result["scored_pairs"], 1)
        self.assertAlmostEqual(result["coverage"], 0.5)
        mean_level = result["train_global_mean_level"]
        self.assertAlmostEqual(result["rmse"]["baseline_global_mean"], abs(5.0 - mean_level), places=6)
        self.assertGreaterEqual(result["rmse"]["als_clipped_1_5"], 0.0)
        self.assertLessEqual(result["rmse"]["als_clipped_1_5"], 4.0)   # sau khi kẹp [1,5], sai số tối đa = 5-1


if __name__ == "__main__":
    unittest.main(verbosity=2)
