"""Tests for ConfigStore database-backed configuration."""

from __future__ import annotations

import pytest

from turnstone.core.config_store import ConfigStore
from turnstone.core.settings_registry import SETTINGS
from turnstone.core.storage._sqlite import SQLiteBackend

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def storage(tmp_path):
    return SQLiteBackend(str(tmp_path / "test.db"))


@pytest.fixture
def store(storage):
    return ConfigStore(storage)


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------


class TestGet:
    def test_returns_registry_default_when_nothing_stored(self, store):
        defn = SETTINGS["tools.timeout"]
        assert store.get("tools.timeout") == defn.default

    def test_returns_stored_value_after_set(self, store):
        store.set("tools.timeout", 60)
        assert store.get("tools.timeout") == 60

    def test_explicit_default_for_unknown_key(self, store):
        # Unknown keys fall back to explicit default
        assert store.get("nonexistent.key", 42) == 42

    def test_none_for_unknown_key_without_default(self, store):
        assert store.get("nonexistent.key") is None


# ---------------------------------------------------------------------------
# set() — validation
# ---------------------------------------------------------------------------


class TestSet:
    def test_rejects_unknown_key(self, store):
        with pytest.raises(ValueError, match="Unknown setting"):
            store.set("bogus.key", "value")

    def test_rejects_out_of_range(self, store):
        with pytest.raises(ValueError, match="minimum"):
            store.set("tools.timeout", 0)

    def test_rejects_above_max(self, store):
        with pytest.raises(ValueError, match="maximum"):
            store.set("tools.timeout", 9999)


# ---------------------------------------------------------------------------
# set() + get() round-trips
# ---------------------------------------------------------------------------


class TestSetGetRoundTrip:
    def test_int(self, store):
        store.set("tools.timeout", 30)
        assert store.get("tools.timeout") == 30
        assert isinstance(store.get("tools.timeout"), int)

    def test_float(self, store):
        store.set("model.temperature", 0.42)
        assert store.get("model.temperature") == 0.42
        assert isinstance(store.get("model.temperature"), float)

    def test_bool(self, store):
        store.set("tools.skip_permissions", True)
        assert store.get("tools.skip_permissions") is True
        store.set("tools.skip_permissions", False)
        assert store.get("tools.skip_permissions") is False

    def test_str(self, store):
        store.set("model.default_alias", "gpt5-prod")
        assert store.get("model.default_alias") == "gpt5-prod"

    def test_task_alias(self, store):
        store.set("model.task_alias", "fast")
        assert store.get("model.task_alias") == "fast"

    def test_task_effort(self, store):
        store.set("model.task_effort", "low")
        assert store.get("model.task_effort") == "low"


# ---------------------------------------------------------------------------
# delete()
# ---------------------------------------------------------------------------


class TestDelete:
    def test_reverts_to_default(self, store):
        store.set("tools.timeout", 30)
        assert store.get("tools.timeout") == 30
        store.delete("tools.timeout")
        defn = SETTINGS["tools.timeout"]
        assert store.get("tools.timeout") == defn.default

    def test_returns_false_for_non_existent(self, store):
        result = store.delete("tools.timeout")
        assert result is False

    def test_rejects_unknown_key(self, store):
        with pytest.raises(ValueError, match="Unknown setting"):
            store.delete("nonexistent.key")


# ---------------------------------------------------------------------------
# reload()
# ---------------------------------------------------------------------------


class TestReload:
    def test_picks_up_external_storage_changes(self, storage, store):
        # Write directly to storage, bypassing ConfigStore
        from turnstone.core.settings_registry import serialize_value

        storage.upsert_system_setting(
            key="tools.timeout",
            value=serialize_value(99),
            node_id="",
            is_secret=False,
            changed_by="external",
        )
        # Not visible yet (cached)
        defn = SETTINGS["tools.timeout"]
        assert store.get("tools.timeout") == defn.default
        # Reload and verify
        store.reload()
        assert store.get("tools.timeout") == 99


# ---------------------------------------------------------------------------
# reload() health signal — fail-open regression coverage
#
# Two fail-open shapes existed before this signal was added:
#   1. A storage query that raises was swallowed with only a log.warning;
#      no consumer could ever tell a reload had failed.
#   2. A storage query that succeeds but returns fewer rows than before
#      (including a drop to zero — the 2026-07-25 incident's shape) left
#      no trace anywhere, not even a log line.
# ---------------------------------------------------------------------------


class TestReloadHealthSignal:
    def test_last_reload_ok_true_after_successful_init(self, store):
        assert store.last_reload_ok is True
        assert store.last_reload_at is not None
        assert store.last_reload_error is None

    def test_last_reload_ok_true_after_successful_explicit_reload(self, store):
        store.reload()
        assert store.last_reload_ok is True
        assert store.last_reload_error is None

    def test_reload_failure_sets_ok_false_and_preserves_cache(self, storage, store):
        # Establish a known-good cached value before storage starts failing.
        store.set("tools.timeout", 42)
        assert store.last_reload_ok is True

        def _raise(*args, **kwargs):
            raise RuntimeError("storage unavailable")

        storage.get_system_settings_bulk = _raise  # type: ignore[method-assign]

        store.reload()

        assert store.last_reload_ok is False
        assert store.last_reload_error is not None
        assert "storage unavailable" in store.last_reload_error
        # Cache is left as-is (stale-but-present), not wiped to {}.
        assert store.get("tools.timeout") == 42

    def test_reload_recovers_ok_after_a_prior_failure(self, storage, store):
        def _raise(*args, **kwargs):
            raise RuntimeError("boom")

        original = storage.get_system_settings_bulk
        storage.get_system_settings_bulk = _raise  # type: ignore[method-assign]
        store.reload()
        assert store.last_reload_ok is False

        storage.get_system_settings_bulk = original  # type: ignore[method-assign]
        store.reload()
        assert store.last_reload_ok is True
        assert store.last_reload_error is None

    def test_first_reload_failure_in_init_leaves_well_defined_state(self, storage):
        """A storage failure on the very first reload() (inside __init__)
        must not raise AttributeError and must leave last_reload_ok=False."""

        def _raise(*args, **kwargs):
            raise RuntimeError("down at boot")

        storage.get_system_settings_bulk = _raise  # type: ignore[method-assign]

        store = ConfigStore(storage)

        assert store.last_reload_ok is False
        assert store.last_reload_error is not None
        # Cache is empty (never successfully loaded) — get() falls back
        # to registry defaults rather than raising.
        defn = SETTINGS["tools.timeout"]
        assert store.get("tools.timeout") == defn.default

    def test_shrink_to_zero_logs_warning(self, storage, store, caplog):
        """The exact 2026-07-25 incident shape: storage query succeeds but
        returns zero rows where it previously returned some. Must not be
        silent."""
        store.set("tools.timeout", 42)
        assert len(store.stored_keys()) == 1

        # Simulate an external wipe: the next bulk query returns nothing,
        # without raising.
        storage.get_system_settings_bulk = lambda node_id="": {}  # type: ignore[method-assign]

        with caplog.at_level("WARNING"):
            store.reload()

        assert store.last_reload_ok is True  # query succeeded — not a failure
        assert len(store.stored_keys()) == 0
        assert any("shrank" in rec.message for rec in caplog.records)

    def test_shrink_partial_also_logs_warning(self, storage, store, caplog):
        store.set("tools.timeout", 42)
        store.set("model.default_alias", "gpt5-prod")
        assert len(store.stored_keys()) == 2

        # Simulate one key silently disappearing from storage underneath us.
        original = storage.get_system_settings_bulk

        def _one_key(node_id=""):
            full = original(node_id=node_id)
            full.pop("model.default_alias", None)
            return full

        storage.get_system_settings_bulk = _one_key  # type: ignore[method-assign]

        with caplog.at_level("WARNING"):
            store.reload()

        assert len(store.stored_keys()) == 1
        assert any("shrank" in rec.message for rec in caplog.records)

    def test_growth_does_not_log_shrink_warning(self, storage, store, caplog):
        with caplog.at_level("WARNING"):
            store.set("tools.timeout", 42)
            store.reload()

        assert not any("shrank" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# all_effective()
# ---------------------------------------------------------------------------


class TestAllEffective:
    def test_merges_stored_with_defaults(self, store):
        store.set("tools.timeout", 30)
        effective = store.all_effective()
        # Stored value
        assert effective["tools.timeout"] == 30
        # Default for unstored
        assert effective["memory.relevance_k"] == SETTINGS["memory.relevance_k"].default
        # All registry keys present
        assert set(effective.keys()) == set(SETTINGS.keys())


# ---------------------------------------------------------------------------
# stored_keys()
# ---------------------------------------------------------------------------


class TestStoredKeys:
    def test_returns_correct_set(self, store):
        assert store.stored_keys() == frozenset()
        store.set("tools.timeout", 30)
        assert store.stored_keys() == frozenset({"tools.timeout"})
        store.set("model.default_alias", "gpt5-prod")
        assert store.stored_keys() == frozenset({"tools.timeout", "model.default_alias"})
        store.delete("tools.timeout")
        assert store.stored_keys() == frozenset({"model.default_alias"})


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------


class TestVersion:
    def test_increments_on_set(self, store):
        v0 = store.version
        store.set("tools.timeout", 30)
        assert store.version == v0 + 1

    def test_increments_on_delete(self, store):
        store.set("tools.timeout", 30)
        v0 = store.version
        store.delete("tools.timeout")
        assert store.version == v0 + 1

    def test_increments_on_reload(self, store):
        v0 = store.version
        store.reload()
        assert store.version == v0 + 1
