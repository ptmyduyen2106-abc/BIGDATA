"""Dependency-free binary ranking metrics and chronological split helpers."""
from __future__ import annotations

import math
from typing import Iterable, Mapping


EVENT_WEIGHTS = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}


def _top_unique(recommended: Iterable[int], k: int) -> list[int]:
    if type(k) is not int or k < 1:
        raise ValueError("k must be a positive integer")
    result, seen = [], set()
    for item in recommended:
        if item not in seen:
            seen.add(item)
            result.append(item)
            if len(result) >= k:
                break
    return result


def precision_at_k(recommended: Iterable[int], relevant: Iterable[int], k: int) -> float:
    actual = set(relevant)
    return sum(item in actual for item in _top_unique(recommended, k)) / k


def recall_at_k(recommended: Iterable[int], relevant: Iterable[int], k: int) -> float:
    actual = set(relevant)
    predicted = _top_unique(recommended, k)
    return sum(item in actual for item in predicted) / len(actual) if actual else 0.0


def ndcg_at_k(recommended: Iterable[int], relevant: Iterable[int], k: int) -> float:
    actual = set(relevant)
    predicted = _top_unique(recommended, k)
    dcg = sum(1.0 / math.log2(position + 2) for position, item in enumerate(predicted) if item in actual)
    ideal = sum(1.0 / math.log2(position + 2) for position in range(min(k, len(actual))))
    return dcg / ideal if ideal else 0.0


def user_ranking_metrics(recommended: Iterable[int], relevant: Iterable[int], k: int) -> dict[str, float]:
    predicted, actual = list(recommended), set(relevant)
    return {"precision_at_k": precision_at_k(predicted, actual, k),
            "recall_at_k": recall_at_k(predicted, actual, k),
            "ndcg_at_k": ndcg_at_k(predicted, actual, k)}


def filter_seen_ranked(candidates: Iterable[Mapping], seen: Iterable[int], k: int) -> list[dict]:
    """Pure deterministic helper matching serving score-desc/item-ID-asc order."""
    if type(k) is not int or k < 1:
        raise ValueError("k must be a positive integer")
    seen_ids, best = set(seen), {}
    for candidate in candidates:
        item, score = candidate["itemid"], candidate["score"]
        if type(item) is not int or item < 0 or not math.isfinite(float(score)):
            raise ValueError("Candidates need nonnegative integer IDs and finite scores")
        if item not in seen_ids:
            best[item] = max(float(score), best.get(item, -math.inf))
    return [{"itemid": item, "score": score}
            for item, score in sorted(best.items(), key=lambda pair: (-pair[1], pair[0]))[:k]]


def chronological_boundaries(timestamps: Iterable[int], fractions=(0.50, 0.70, 0.85)) -> tuple[int, ...]:
    """Exact helper for unit tests/small data; equal timestamps never straddle a boundary."""
    values = sorted(timestamps)
    if not values or any(type(value) is not int for value in values):
        raise ValueError("timestamps must be a nonempty integer sequence")
    if not fractions or any(not 0 < fraction < 1 for fraction in fractions) or sorted(set(fractions)) != list(fractions):
        raise ValueError("fractions must be distinct and increasing in (0,1)")
    boundaries = tuple(values[min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))]
                       for fraction in fractions)
    if len(set(boundaries)) != len(boundaries) or boundaries[-1] >= values[-1]:
        raise ValueError("Not enough distinct timestamps for strict chronological folds")
    return boundaries


def split_label(timestamp: int, train_end: int, validation_end: int) -> str:
    if train_end >= validation_end:
        raise ValueError("train_end must precede validation_end")
    return "train" if timestamp <= train_end else "validation" if timestamp <= validation_end else "test"


def rolling_folds(boundaries: Iterable[int]) -> list[dict]:
    values = tuple(boundaries)
    if len(values) < 3 or any(first >= second for first, second in zip(values, values[1:])):
        raise ValueError("At least three strictly increasing boundaries are required")
    return [{"train_end_inclusive": first, "validation_start_exclusive": first,
             "validation_end_inclusive": second} for first, second in zip(values[:-1], values[1:])]
