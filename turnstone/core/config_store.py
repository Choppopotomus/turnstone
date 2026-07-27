"""Database-backed configuration store with in-memory caching.

Provides a unified ``get()`` API for runtime config access.  Settings
are loaded from the ``system_settings`` table on init and cached in
memory.  Call ``reload()`` to refresh from storage (e.g. on MQ
invalidation event).

Precedence chain for **server** entry point:
  CLI flag  >  ConfigStore (this)  >  registry default

The server's ``apply_config()`` no longer loads ConfigStore-managed
sections from config.toml, so there is no precedence conflict.
config.toml values for these sections are ignored (with a warning).

The **CLI** entry point still reads config.toml directly (no
ConfigStore) — it is a standalone tool, not a cluster node.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from turnstone.core.log import get_logger
from turnstone.core.settings_registry import (
    SETTINGS,
    deserialize_value,
    serialize_value,
    validate_key,
    validate_value,
)

if TYPE_CHECKING:
    from turnstone.core.storage._protocol import StorageBackend

log = get_logger(__name__)

_UNSET: Any = object()


class ConfigStore:
    """Runtime config accessor with database-backed storage.

    Thread-safe.  Reads are lock-free after initialization (dict
    lookup on an immutable snapshot).  Writes acquire a lock,
    update storage, and swap the cache atomically.
    """

    def __init__(self, storage: StorageBackend, node_id: str = "") -> None:
        self._storage = storage
        self._node_id = node_id
        self._cache: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._version = 0
        # Health signal for reload() — set before the first reload() call
        # below so a first-load failure leaves well-defined attributes
        # (last_reload_ok=False, cache stays {}) instead of raising
        # AttributeError from inside the except branch.
        self._last_reload_ok = True
        self._last_reload_at: str | None = None
        self._last_reload_error: str | None = None
        self.reload()

    @property
    def storage(self) -> StorageBackend:
        """Read-only access to the underlying storage backend."""
        return self._storage

    @property
    def version(self) -> int:
        """Monotonic counter incremented on every cache update."""
        return self._version

    @property
    def last_reload_ok(self) -> bool:
        """Whether the most recent :meth:`reload` completed without error.

        ``True`` initially (optimistic default) until the first
        :meth:`reload` call in ``__init__`` resolves it one way or the
        other. Consumers (e.g. the ``/health`` endpoint) should treat
        ``False`` as a real degraded signal — it means storage could not
        be queried at all, and the cache may be stale.
        """
        return self._last_reload_ok

    @property
    def last_reload_at(self) -> str | None:
        """UTC ISO-8601 timestamp of the most recent reload attempt (ok or not)."""
        return self._last_reload_at

    @property
    def last_reload_error(self) -> str | None:
        """String repr of the exception from the most recent failed reload, if any."""
        return self._last_reload_error

    def reload(self) -> None:
        """Load all settings from storage into the in-memory cache.

        On a storage query failure, the exception is logged and
        ``last_reload_ok`` is set to ``False`` — the cache is left as-is
        (stale-but-present beats empty) but the failure is now a real,
        checkable signal rather than only a log line.

        On a successful query that returns *fewer* keys than the current
        cache (including a drop to zero), a warning is logged — ``set()``
        and ``delete()`` mutate the cache directly and never call
        ``reload()``, so a shrink observed here means storage changed
        out from under this process (external delete, or a wipe). This
        is not refused — an operator may have made a legitimate change
        on another node — but it must not pass silently.
        """
        old_count = len(self._cache)
        now = datetime.now(UTC).isoformat()
        try:
            raw = self._storage.get_system_settings_bulk(node_id=self._node_id)
        except Exception as exc:
            with self._lock:
                self._last_reload_ok = False
                self._last_reload_error = repr(exc)
                self._last_reload_at = now
            log.warning("Failed to load settings from storage", exc_info=True)
            return
        new_cache: dict[str, Any] = {}
        for key, json_val in raw.items():
            try:
                new_cache[key] = deserialize_value(key, json_val)
            except (ValueError, KeyError):
                log.warning("Skipping invalid setting: %s", key)
        new_count = len(new_cache)
        if new_count < old_count:
            log.warning(
                "ConfigStore.reload(): cache shrank from %d to %d key(s) — storage "
                "returned fewer settings than the current cache. This reload path is "
                "never reached by set()/delete(), so this reflects a real change in "
                "storage (external delete, or a wipe) rather than local mutation.",
                old_count,
                new_count,
            )
        with self._lock:
            self._cache = new_cache
            self._version += 1
            self._last_reload_ok = True
            self._last_reload_error = None
            self._last_reload_at = now

    def get(self, key: str, default: Any = _UNSET) -> Any:
        """Get a setting value from cache.

        Returns the stored value if present, otherwise the registry
        default.  If *default* is provided, it takes precedence over
        the registry default for unknown keys.
        """
        cache = self._cache  # snapshot for lock-free read
        if key in cache:
            return cache[key]
        if default is not _UNSET:
            return default
        defn = SETTINGS.get(key)
        return defn.default if defn else None

    def set(self, key: str, value: Any, changed_by: str = "") -> Any:
        """Write a setting to storage and update cache.

        Returns the typed value after validation.
        """
        defn = validate_key(key)
        typed_value = validate_value(key, value)
        self._storage.upsert_system_setting(
            key=key,
            value=serialize_value(typed_value),
            node_id=self._node_id,
            is_secret=defn.is_secret,
            changed_by=changed_by,
        )
        with self._lock:
            self._cache = {**self._cache, key: typed_value}
            self._version += 1
        return typed_value

    def delete(self, key: str) -> bool:
        """Remove a setting from storage (reverts to default)."""
        validate_key(key)  # reject unknown keys
        result = self._storage.delete_system_setting(key, node_id=self._node_id)
        with self._lock:
            new_cache = dict(self._cache)
            new_cache.pop(key, None)
            self._cache = new_cache
            self._version += 1
        return result

    def all_effective(self) -> dict[str, Any]:
        """Return all settings with their effective values.

        Merges stored values with registry defaults.
        """
        cache = self._cache
        result: dict[str, Any] = {}
        for key, defn in SETTINGS.items():
            result[key] = cache.get(key, defn.default)
        return result

    def stored_keys(self) -> frozenset[str]:
        """Return the keys that have explicit values in storage."""
        return frozenset(self._cache.keys())
