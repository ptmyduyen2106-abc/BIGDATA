"""Phase 5: Spark MLlib ALS recommender (tasks 16, 17, 18).

Thin entry point over ``pipeline.train`` / ``pipeline.ranking`` so Phase 5 has its
own folder like phase2/ and phase3/ without duplicating code that
``pipeline.runner`` already imports.
"""
from pipeline.ranking import EVENT_WEIGHTS, rolling_folds  # noqa: F401
from pipeline.train import (  # noqa: F401
    evaluate_ranking,
    fit_als_pipeline,
    prepare_interactions,
    recommend_unseen,
    train_model,
)

__all__ = [
    "EVENT_WEIGHTS", "rolling_folds", "evaluate_ranking", "fit_als_pipeline",
    "prepare_interactions", "recommend_unseen", "train_model",
]
