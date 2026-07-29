from __future__ import annotations

from collections.abc import Callable
from typing import Any


class RelationCache:
    """Versioned cache for model relationships discovered from global registries.

    The existing model keeps every entity in class-level registries. Relationship
    properties previously scanned the complete registry every time or used stale
    cached_property values. This cache rebuilds a relationship only when the
    number of related objects changes.
    """

    _owner_cache: dict[tuple[str, int, str], tuple[int, Any]] = {}
    _global_cache: dict[str, tuple[int, Any]] = {}

    @classmethod
    def get(
        cls,
        owner: Any,
        relation_name: str,
        version: int,
        factory: Callable[[], Any],
    ) -> Any:
        key = (owner.__class__.__name__, owner.id, relation_name)
        cached = cls._owner_cache.get(key)
        if cached is None or cached[0] != version:
            value = factory()
            cls._owner_cache[key] = (version, value)
            return value
        return cached[1]

    @classmethod
    def get_global(
        cls,
        cache_name: str,
        version: int,
        factory: Callable[[], Any],
    ) -> Any:
        cached = cls._global_cache.get(cache_name)
        if cached is None or cached[0] != version:
            value = factory()
            cls._global_cache[cache_name] = (version, value)
            return value
        return cached[1]

    @classmethod
    def clear(cls) -> None:
        cls._owner_cache.clear()
        cls._global_cache.clear()
