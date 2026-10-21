"""Optional online histories and a version-scoped, short-lived Redis cache."""
from __future__ import annotations

import json

from service.store import Settings, valid_id, validate_items


class Backends:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.redis = None
        self.mongo = None
        self.errors: dict[str, str] = {}

    def _redis(self):
        if not self.settings.redis_url:
            return None
        if self.redis is None:
            import redis
            self.redis = redis.Redis.from_url(self.settings.redis_url, decode_responses=True,
                                             socket_connect_timeout=0.3, socket_timeout=0.3)
        return self.redis

    def _mongo(self):
        if not self.settings.mongo_uri:
            return None
        if self.mongo is None:
            from pymongo import MongoClient
            self.mongo = MongoClient(self.settings.mongo_uri, serverSelectionTimeoutMS=350,
                                     connectTimeoutMS=350, socketTimeoutMS=350)
        return self.mongo

    @staticmethod
    def cache_key(run_id: str, version: str, visitorid: int, k: int) -> str:
        return f"bigdata:{run_id}:recommendations:{version}:{visitorid}:{k}"

    def history(self, run_id: str, visitorid: int) -> tuple[set[int], list[str], list[str]]:
        seen: set[int] = set()
        sources, warnings = [], []
        try:
            client = self._redis()
            if client is not None:
                values = client.smembers(f"bigdata:{run_id}:seen:{visitorid}")
                for value in values:
                    if str(value).isdigit() and valid_id(int(value)):
                        seen.add(int(value))
                sources.append("redis")
                self.errors.pop("redis", None)
        except Exception:
            self.errors["redis"] = "unavailable"
            warnings.append("Redis unavailable; using other scoped histories")
        try:
            client = self._mongo()
            if client is not None:
                row = client[self.settings.mongo_db].user_features.find_one(
                    {"run_id": run_id, "visitorid": visitorid}, {"seen_items": 1})
                if row:
                    seen.update(item for item in row.get("seen_items", []) if valid_id(item))
                sources.append("mongodb")
                self.errors.pop("mongodb", None)
        except Exception:
            self.errors["mongodb"] = "unavailable"
            warnings.append("MongoDB unavailable; using other scoped histories")
        return seen, sources, warnings

    def cache_get(self, run_id: str, version: str, visitorid: int, k: int) -> dict | None:
        try:
            client = self._redis()
            if client is None:
                return None
            value = client.get(self.cache_key(run_id, version, visitorid, k))
            if not value:
                return None
            row = json.loads(value)
            if (not isinstance(row, dict) or row.get("run_id") != run_id
                    or row.get("model_version") != version or row.get("visitorid") != visitorid
                    or row.get("k") != k or row.get("source") not in {"als_bundle", "popular_fallback"}):
                return None
            validate_items(row.get("items"), "cached items")
            return row
        except Exception:
            self.errors["redis"] = "unavailable_or_invalid_cache"
            return None

    def cache_set(self, row: dict) -> None:
        try:
            client = self._redis()
            if client is not None:
                client.setex(self.cache_key(row["run_id"], row["model_version"], row["visitorid"], row["k"]),
                             self.settings.cache_ttl, json.dumps(row, allow_nan=False))
        except Exception:
            self.errors["redis"] = "unavailable"

    def statuses(self) -> dict:
        result = {}
        for name, configured, get_client in (
            ("redis", self.settings.redis_url, self._redis),
            ("mongodb", self.settings.mongo_uri, self._mongo),
        ):
            if not configured:
                result[name] = {"enabled": False, "status": "disabled", "required_for_ready": False}
                continue
            try:
                client = get_client()
                client.ping() if name == "redis" else client.admin.command("ping")
                result[name] = {"enabled": True, "status": "ok", "required_for_ready": False}
            except Exception:
                result[name] = {"enabled": True, "status": "unavailable", "required_for_ready": False}
        return result

    def close(self) -> None:
        for client in (self.redis, self.mongo):
            if client is not None:
                client.close()
