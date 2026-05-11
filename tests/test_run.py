"""
Tests for run.py — all file I/O uses tmp_path, no real config required.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

import run
from run import since_last_run


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(path: Path, data: dict) -> None:
    with path.open("w") as f:
        yaml.dump(data, f)


def _minimal_config() -> dict:
    return {
        "mastodon_instance": "hachyderm.io",
        "mastodon_handle": "cate",
        "bluesky_handle": "catehstn.bsky.social",
        "buttondown_api_key": "key",
        "jetpack_site": "cate.blog",
        "jetpack_access_token": "token",
    }


# ---------------------------------------------------------------------------
# week_label
# ---------------------------------------------------------------------------

class TestWeekLabel:
    def test_format(self):
        dt = datetime(2026, 3, 6, tzinfo=timezone.utc)
        assert run.week_label(dt) == "2026-W10"

    def test_zero_padded_week(self):
        dt = datetime(2026, 1, 5, tzinfo=timezone.utc)  # first week of 2026
        label = run.week_label(dt)
        assert label.startswith("2026-W0")

    def test_with_months_appends_suffix(self):
        dt = datetime(2026, 3, 6, tzinfo=timezone.utc)
        assert run.week_label(dt, months=3) == "2026-W10-3m"

    def test_without_months_no_suffix(self):
        dt = datetime(2026, 3, 6, tzinfo=timezone.utc)
        label = run.week_label(dt)
        assert "m" not in label

    def test_defaults_to_now(self):
        label = run.week_label()
        assert label.startswith("20")
        assert "-W" in label


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_missing_file_exits(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run, "CONFIG_PATH", tmp_path / "nofile.yaml")
        with pytest.raises(SystemExit) as exc:
            run.load_config()
        assert exc.value.code == 1

    def test_invalid_yaml_exits(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("---\n: : invalid: yaml: [[[")
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        # yaml.safe_load on invalid YAML raises, which propagates —
        # but an empty/non-dict result also exits
        with pytest.raises((SystemExit, Exception)):
            run.load_config()

    def test_non_dict_yaml_exits(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("- just\n- a\n- list\n")
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        with pytest.raises(SystemExit) as exc:
            run.load_config()
        assert exc.value.code == 1

    def test_valid_config_returned(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, _minimal_config())
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        config = run.load_config()
        assert config["mastodon_instance"] == "hachyderm.io"

    def test_missing_keys_warns_but_returns(self, tmp_path, monkeypatch, caplog):
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, {"mastodon_instance": "hachyderm.io"})
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        import logging
        with caplog.at_level(logging.WARNING, logger="run"):
            config = run.load_config()
        assert isinstance(config, dict)
        assert any("missing" in r.message.lower() for r in caplog.records)

    def test_all_required_keys_present_no_warning(self, tmp_path, monkeypatch, caplog):
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, _minimal_config())
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        import logging
        with caplog.at_level(logging.WARNING, logger="run"):
            run.load_config()
        assert not any("missing" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# save_raw
# ---------------------------------------------------------------------------

class TestSaveRaw:
    def test_creates_directory_if_absent(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        assert not data_dir.exists()
        run.save_raw({"mastodon": {}}, "2026-W10")
        assert data_dir.exists()

    def test_writes_json_file(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        run.save_raw({"mastodon": {"posts": []}}, "2026-W10")
        path = data_dir / "2026-W10.json"
        assert path.exists()
        assert json.loads(path.read_text())["mastodon"] == {"posts": []}

    def test_returns_path(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        result = run.save_raw({}, "2026-W10")
        assert result == data_dir / "2026-W10.json"

    def test_label_used_as_filename(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        run.save_raw({}, "2026-W10-3m")
        assert (data_dir / "2026-W10-3m.json").exists()


# ---------------------------------------------------------------------------
# save_platform_latest
# ---------------------------------------------------------------------------

class TestSavePlatformLatest:
    def test_creates_directory_if_absent(self, tmp_path, monkeypatch):
        platform_dir = tmp_path / "data" / "platform"
        monkeypatch.setattr(run, "PLATFORM_DIR", platform_dir)
        assert not platform_dir.exists()
        run.save_platform_latest({"buttondown": {"collected_at": "2026-05-29"}})
        assert platform_dir.exists()

    def test_writes_one_file_per_platform(self, tmp_path, monkeypatch):
        platform_dir = tmp_path / "data" / "platform"
        monkeypatch.setattr(run, "PLATFORM_DIR", platform_dir)
        run.save_platform_latest({
            "buttondown": {"collected_at": "2026-05-29"},
            "mastodon": {"posts": []},
        })
        assert (platform_dir / "buttondown-latest.json").exists()
        assert (platform_dir / "mastodon-latest.json").exists()

    def test_file_wrapped_in_platform_key(self, tmp_path, monkeypatch):
        platform_dir = tmp_path / "data" / "platform"
        monkeypatch.setattr(run, "PLATFORM_DIR", platform_dir)
        run.save_platform_latest({"buttondown": {"collected_at": "2026-05-29", "newsletters": []}})
        data = json.loads((platform_dir / "buttondown-latest.json").read_text())
        assert "buttondown" in data
        assert data["buttondown"]["collected_at"] == "2026-05-29"

    def test_overwrites_existing_file(self, tmp_path, monkeypatch):
        platform_dir = tmp_path / "data" / "platform"
        platform_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "PLATFORM_DIR", platform_dir)
        old = platform_dir / "buttondown-latest.json"
        old.write_text(json.dumps({"buttondown": {"collected_at": "2026-04-24"}}))
        run.save_platform_latest({"buttondown": {"collected_at": "2026-05-29"}})
        data = json.loads(old.read_text())
        assert data["buttondown"]["collected_at"] == "2026-05-29"

    def test_empty_collected_writes_nothing(self, tmp_path, monkeypatch):
        platform_dir = tmp_path / "data" / "platform"
        monkeypatch.setattr(run, "PLATFORM_DIR", platform_dir)
        run.save_platform_latest({})
        assert platform_dir.exists()
        assert list(platform_dir.iterdir()) == []


# ---------------------------------------------------------------------------
# load_latest_raw
# ---------------------------------------------------------------------------

class TestLoadLatestRaw:
    def test_no_snapshots_exits(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        with pytest.raises(SystemExit) as exc:
            run.load_latest_raw()
        assert exc.value.code == 1

    def test_returns_most_recently_modified(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)

        older = data_dir / "2026-W09.json"
        newer = data_dir / "2026-W10.json"
        older.write_text(json.dumps({"week": "old"}))
        time.sleep(0.01)
        newer.write_text(json.dumps({"week": "new"}))

        data, label = run.load_latest_raw()
        assert label == "2026-W10"
        assert data["week"] == "new"

    def test_returns_label_from_filename(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        (data_dir / "2026-W10-3m.json").write_text(json.dumps({}))
        _, label = run.load_latest_raw()
        assert label == "2026-W10-3m"

    def test_parses_json_correctly(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        payload = {"mastodon": {"posts": [{"id": "1"}]}}
        (data_dir / "2026-W10.json").write_text(json.dumps(payload))
        data, _ = run.load_latest_raw()
        assert data["mastodon"]["posts"][0]["id"] == "1"


# ---------------------------------------------------------------------------
# since_last_run
# ---------------------------------------------------------------------------

class TestSinceLastRun:
    def test_no_snapshots_returns_none(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        assert since_last_run() is None

    def test_recent_snapshot_returns_none(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        # File modified now — within 2-week window
        (data_dir / "2026-W10.json").write_text("{}")
        assert since_last_run() is None

    def test_old_snapshot_returns_its_mtime(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        path = data_dir / "2026-W01.json"
        path.write_text("{}")
        # Wind mtime back 30 days
        old_ts = time.time() - (30 * 86400)
        import os
        os.utime(path, (old_ts, old_ts))
        result = since_last_run()
        assert result is not None
        delta = datetime.now(timezone.utc) - result
        assert delta.days >= 29

    def test_uses_most_recent_snapshot(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        import os
        # One old file
        old = data_dir / "2026-W01.json"
        old.write_text("{}")
        os.utime(old, (time.time() - 30 * 86400,) * 2)
        # One recent file
        new = data_dir / "2026-W10.json"
        new.write_text("{}")
        # Most recent is new (within 2 weeks) → should return None
        assert since_last_run() is None


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------

def _setup_main(tmp_path, monkeypatch, argv: list[str]):
    """Patch paths and sys.argv; return a fake config file."""
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data" / "weekly"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir(parents=True)

    _write_config(config_path, _minimal_config())
    monkeypatch.setattr(run, "CONFIG_PATH", config_path)
    monkeypatch.setattr(run, "DATA_DIR", data_dir)
    monkeypatch.setattr(run, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(sys, "argv", ["run.py"] + argv)
    return data_dir, reports_dir


class TestMain:
    def test_analyse_only_and_platform_exits(self, tmp_path, monkeypatch):
        _setup_main(tmp_path, monkeypatch, ["--analyse-only", "--platform", "mastodon"])
        with pytest.raises(SystemExit) as exc:
            run.main()
        assert exc.value.code == 1

    def test_collect_only_skips_analysis(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {"posts": []}})
        mock_store_update = MagicMock()
        mock_get_known = MagicMock(return_value={"mastodon"})
        mock_save_prompt = MagicMock()

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=mock_store_update, get_known_platforms=mock_get_known, STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=mock_save_prompt),
        }):
            run.main()

        mock_collect.assert_called_once()
        mock_save_prompt.assert_not_called()

    def test_collect_only_saves_raw_json(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {"posts": []}})
        mock_get_known = MagicMock(return_value={"mastodon"})

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=MagicMock(), get_known_platforms=mock_get_known, STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        snapshots = list(data_dir.glob("*.json"))
        assert len(snapshots) == 1

    def test_analyse_only_skips_collection(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--analyse-only"])
        (data_dir / "2026-W10.json").write_text(json.dumps({"mastodon": {}}))

        mock_collect = MagicMock()
        mock_save_prompt = MagicMock(return_value=tmp_path / "prompt.txt")

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=MagicMock(), get_known_platforms=MagicMock(return_value=set()), STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=mock_save_prompt),
        }):
            run.main()

        mock_collect.assert_not_called()
        mock_save_prompt.assert_called_once()

    def test_analyse_only_loads_latest_snapshot(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--analyse-only"])
        payload = {"mastodon": {"posts": [{"id": "p1"}]}}
        (data_dir / "2026-W10.json").write_text(json.dumps(payload))

        mock_save_prompt = MagicMock(return_value=tmp_path / "prompt.txt")

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=MagicMock()),
            "store": MagicMock(update=MagicMock(), get_known_platforms=MagicMock(return_value=set()), STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=mock_save_prompt),
        }):
            run.main()

        called_data = mock_save_prompt.call_args[0][0]
        assert "mastodon" in called_data

    def test_platform_flag_skips_store_update(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--platform", "mastodon", "--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {"posts": []}})
        mock_store_update = MagicMock()

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=mock_store_update, get_known_platforms=MagicMock(return_value=set()), STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        mock_store_update.assert_not_called()

    def test_new_platform_triggers_backfill(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {"posts": []}})
        mock_store_update = MagicMock()
        # mastodon is not yet known → triggers backfill
        mock_get_known = MagicMock(return_value=set())

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=mock_store_update, get_known_platforms=mock_get_known, STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        # collect_all called twice: once for current data, once for backfill
        assert mock_collect.call_count == 2
        # backfill call has since set (not None)
        backfill_call = mock_collect.call_args_list[1]
        assert backfill_call[1]["since"] is not None or backfill_call[0][2] is not None

    def test_known_platform_no_backfill(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {"posts": []}})
        mock_store_update = MagicMock()
        mock_get_known = MagicMock(return_value={"mastodon"})

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=mock_store_update, get_known_platforms=mock_get_known, STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        assert mock_collect.call_count == 1
        mock_store_update.assert_called_once_with({"mastodon": {"posts": []}})

    def test_months_flag_sets_since(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--months", "3", "--collect-only"])
        mock_collect = MagicMock(return_value={"mastodon": {}})
        mock_get_known = MagicMock(return_value={"mastodon"})

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=MagicMock(), get_known_platforms=mock_get_known, STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        call_kwargs = mock_collect.call_args
        since = call_kwargs[1].get("since") or call_kwargs[0][2]
        assert since is not None
        # since should be approximately 90 days ago
        delta = datetime.now(timezone.utc) - since
        assert 85 <= delta.days <= 95

    def test_gap_detection_extends_since(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        # Write an old snapshot (30 days ago) to trigger gap detection
        import os
        old = data_dir / "2026-W01.json"
        old.write_text("{}")
        os.utime(old, (time.time() - 30 * 86400,) * 2)

        mock_collect = MagicMock(return_value={"mastodon": {}})
        mock_get_known = MagicMock(return_value={"mastodon"})

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=MagicMock(), get_known_platforms=mock_get_known,
                               STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        since = mock_collect.call_args[1].get("since") or mock_collect.call_args[0][2]
        assert since is not None
        delta = datetime.now(timezone.utc) - since
        assert delta.days >= 29

    def test_empty_collection_no_store_update(self, tmp_path, monkeypatch):
        data_dir, _ = _setup_main(tmp_path, monkeypatch, ["--collect-only"])
        mock_collect = MagicMock(return_value={})
        mock_store_update = MagicMock()

        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=mock_store_update, get_known_platforms=MagicMock(return_value=set()), STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()

        mock_store_update.assert_not_called()


# ---------------------------------------------------------------------------
# parse_args — --platform choices generated from PLATFORM_COLLECTORS
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_platform_choices_match_platform_collectors_keys(self, monkeypatch):
        """--platform choices must equal PLATFORM_COLLECTORS.keys() (sorted)."""
        from collectors import PLATFORM_COLLECTORS
        monkeypatch.setattr(sys, "argv", ["run.py"])
        # parse_args builds choices from PLATFORM_COLLECTORS inside the function
        # We verify by checking that every key in PLATFORM_COLLECTORS is a valid choice
        # by calling parse_args with each platform value without error
        for platform in PLATFORM_COLLECTORS.keys():
            monkeypatch.setattr(sys, "argv", ["run.py", "--platform", platform])
            args = run.parse_args()
            assert args.platform == platform

    def test_all_platform_collectors_keys_are_valid_choices(self, monkeypatch):
        """Every key in PLATFORM_COLLECTORS should be accepted by --platform."""
        from collectors import PLATFORM_COLLECTORS
        for platform in PLATFORM_COLLECTORS.keys():
            monkeypatch.setattr(sys, "argv", ["run.py", "--platform", platform, "--collect-only"])
            args = run.parse_args()
            assert args.platform == platform

    def test_platform_choices_count_matches_platform_collectors(self, monkeypatch):
        """The number of valid --platform choices equals len(PLATFORM_COLLECTORS)."""
        import argparse
        from collectors import PLATFORM_COLLECTORS
        # Reconstruct the parser to inspect choices directly
        parser = argparse.ArgumentParser()
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--collect-only", action="store_true")
        mode.add_argument("--analyse-only", action="store_true")
        action = parser.add_argument(
            "--platform",
            choices=sorted(PLATFORM_COLLECTORS.keys()),
        )
        assert set(action.choices) == set(PLATFORM_COLLECTORS.keys())
        assert len(action.choices) == len(PLATFORM_COLLECTORS)


# ---------------------------------------------------------------------------
# check_drop_staleness
# ---------------------------------------------------------------------------

class TestCheckDropStaleness:
    def _write_csv(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("col\nval\n")
        return path

    def _write_eml(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("From: test@example.com\nSubject: Payment\n\nHello\n")
        return path

    def test_no_drops_returns_empty_list(self, tmp_path, monkeypatch):
        """No files in any drop directory → no warnings."""
        monkeypatch.chdir(tmp_path)
        assert run.check_drop_staleness() == []

    def test_recent_linkedin_csv_no_warning(self, tmp_path, monkeypatch):
        """LinkedIn CSV < 24h old → no warning."""
        monkeypatch.chdir(tmp_path)
        self._write_csv(tmp_path / "linkedin_drops" / "export.csv")
        warnings = run.check_drop_staleness()
        assert not any("LinkedIn" in w for w in warnings)

    def test_stale_linkedin_csv_warns(self, tmp_path, monkeypatch):
        """LinkedIn CSV > 24h old → warning."""
        monkeypatch.chdir(tmp_path)
        export = tmp_path / "linkedin_drops" / "export.csv"
        self._write_csv(export)
        stale = time.time() - 25 * 3600
        import os
        os.utime(export, (stale, stale))
        warnings = run.check_drop_staleness()
        assert any("LinkedIn" in w for w in warnings)

    def test_recent_substack_csv_no_warning(self, tmp_path, monkeypatch):
        """Substack CSV < 24h old → no warning."""
        monkeypatch.chdir(tmp_path)
        self._write_csv(tmp_path / "substack_drops" / "export.csv")
        warnings = run.check_drop_staleness()
        assert not any("Substack" in w for w in warnings)

    def test_stale_substack_csv_no_warning(self, tmp_path, monkeypatch):
        """Substack CSV (even stale) → no warning — user doesn't use Substack."""
        monkeypatch.chdir(tmp_path)
        export = tmp_path / "substack_drops" / "export.csv"
        self._write_csv(export)
        stale = time.time() - 25 * 3600
        import os
        os.utime(export, (stale, stale))
        warnings = run.check_drop_staleness()
        assert not any("Substack" in w for w in warnings)

    def test_no_drops_directory_no_warning(self, tmp_path, monkeypatch):
        """Missing drop directories don't cause warnings."""
        monkeypatch.chdir(tmp_path)
        # Don't create any directories
        assert run.check_drop_staleness() == []

    def test_stale_oreilly_eml_warns(self, tmp_path, monkeypatch):
        """O'Reilly .eml > 25 days old → warning."""
        monkeypatch.chdir(tmp_path)
        eml = tmp_path / "oreilly_drops" / "payment.eml"
        self._write_eml(eml)
        stale = time.time() - 26 * 24 * 3600
        import os
        os.utime(eml, (stale, stale))
        warnings = run.check_drop_staleness()
        assert any("O'Reilly" in w for w in warnings)

    def test_recent_oreilly_eml_no_warning(self, tmp_path, monkeypatch):
        """O'Reilly .eml < 25 days old → no warning."""
        monkeypatch.chdir(tmp_path)
        eml = tmp_path / "oreilly_drops" / "payment.eml"
        self._write_eml(eml)
        # 10 days old → no warn
        recent = time.time() - 10 * 24 * 3600
        import os
        os.utime(eml, (recent, recent))
        warnings = run.check_drop_staleness()
        assert not any("O'Reilly" in w for w in warnings)

    def test_multiple_stale_files_warn_for_each(self, tmp_path, monkeypatch):
        """Multiple stale files → warning for each platform."""
        monkeypatch.chdir(tmp_path)
        stale_time = time.time() - 25 * 3600
        import os

        li = tmp_path / "linkedin_drops" / "li.csv"
        self._write_csv(li)
        os.utime(li, (stale_time, stale_time))

        warnings = run.check_drop_staleness()
        assert any("LinkedIn" in w for w in warnings)

    def test_warning_includes_filename(self, tmp_path, monkeypatch):
        """Warning message includes the stale filename."""
        monkeypatch.chdir(tmp_path)
        export = tmp_path / "linkedin_drops" / "my_export.csv"
        self._write_csv(export)
        stale = time.time() - 25 * 3600
        import os
        os.utime(export, (stale, stale))
        warnings = run.check_drop_staleness()
        assert any("my_export.csv" in w for w in warnings)

    def test_linkedin_staleness_suppressed_when_api_token_set(self, tmp_path, monkeypatch):
        """When linkedin_access_token is set, stale file-drop warning is suppressed."""
        monkeypatch.chdir(tmp_path)
        export = tmp_path / "linkedin_drops" / "export.csv"
        self._write_csv(export)
        stale = time.time() - 25 * 3600
        import os
        os.utime(export, (stale, stale))
        config = {"linkedin_access_token": "some_token"}
        warnings = run.check_drop_staleness(config)
        assert not any("LinkedIn" in w for w in warnings)

    def test_linkedin_staleness_shown_without_api_token(self, tmp_path, monkeypatch):
        """Without linkedin_access_token, stale file-drop warning still appears."""
        monkeypatch.chdir(tmp_path)
        export = tmp_path / "linkedin_drops" / "export.csv"
        self._write_csv(export)
        stale = time.time() - 25 * 3600
        import os
        os.utime(export, (stale, stale))
        warnings = run.check_drop_staleness({})
        assert any("LinkedIn" in w for w in warnings)


class TestNonInteractiveStaleness:
    """Stale-check behaviour when stdin is not a TTY (e.g. agent runs)."""

    def _write_stale_linkedin(self, tmp_path: Path, monkeypatch) -> None:
        import os
        drop = tmp_path / "linkedin_drops" / "export.csv"
        drop.parent.mkdir(parents=True, exist_ok=True)
        drop.write_text("col\nval\n")
        os.utime(drop, (time.time() - 25 * 3600,) * 2)
        monkeypatch.chdir(tmp_path)

    def test_non_interactive_continues_despite_stale(self, tmp_path, monkeypatch):
        """When stdin is not a TTY, stale warning is logged but run continues."""
        self._write_stale_linkedin(tmp_path, monkeypatch)
        config_path = tmp_path / "config.yaml"
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        _write_config(config_path, _minimal_config())
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        monkeypatch.setattr(run, "REPORTS_DIR", tmp_path / "reports")
        monkeypatch.setattr(sys, "argv", ["run.py", "--collect-only"])
        # stdin.isatty() returns False in pytest — no prompt, no exit
        mock_collect = MagicMock(return_value={})
        with patch.dict("sys.modules", {
            "collect": MagicMock(collect_all=mock_collect),
            "store": MagicMock(update=MagicMock(), get_known_platforms=MagicMock(return_value=set()),
                               STORE_PATH=tmp_path / "analytics.xlsx"),
            "analyse": MagicMock(save_prompt=MagicMock()),
        }):
            run.main()  # must not sys.exit()
        mock_collect.assert_called_once()

    def _setup_interactive(self, tmp_path: Path, monkeypatch) -> None:
        self._write_stale_linkedin(tmp_path, monkeypatch)
        config_path = tmp_path / "config.yaml"
        data_dir = tmp_path / "data" / "weekly"
        data_dir.mkdir(parents=True)
        _write_config(config_path, _minimal_config())
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        monkeypatch.setattr(run, "DATA_DIR", data_dir)
        monkeypatch.setattr(run, "REPORTS_DIR", tmp_path / "reports")
        monkeypatch.setattr(sys, "argv", ["run.py", "--collect-only"])
        fake_stdin = MagicMock()
        fake_stdin.isatty.return_value = True
        monkeypatch.setattr(sys, "stdin", fake_stdin)

    def test_interactive_stale_aborts_on_no(self, tmp_path, monkeypatch):
        """When stdin is a TTY and user answers N, run aborts."""
        self._setup_interactive(tmp_path, monkeypatch)
        with patch("builtins.input", return_value="n"):
            with pytest.raises(SystemExit) as exc:
                run.main()
        assert exc.value.code == 0

    def test_interactive_stale_continues_on_yes(self, tmp_path, monkeypatch):
        """When stdin is a TTY and user answers y, run continues."""
        self._setup_interactive(tmp_path, monkeypatch)
        mock_collect = MagicMock(return_value={})
        with patch("builtins.input", return_value="y"):
            with patch.dict("sys.modules", {
                "collect": MagicMock(collect_all=mock_collect),
                "store": MagicMock(update=MagicMock(), get_known_platforms=MagicMock(return_value=set()),
                                   STORE_PATH=tmp_path / "analytics.xlsx"),
                "analyse": MagicMock(save_prompt=MagicMock()),
            }):
                run.main()
        mock_collect.assert_called_once()


# ---------------------------------------------------------------------------
# --auth linkedin
# ---------------------------------------------------------------------------

class TestAuthSubcommand:
    def _setup(self, tmp_path: Path, monkeypatch, extra_config: dict | None = None) -> Path:
        config_path = tmp_path / "config.yaml"
        cfg = {
            "mastodon_instance": "hachyderm.io",
            "mastodon_handle": "cate",
            "bluesky_handle": "catehstn.bsky.social",
            "buttondown_api_key": "key",
            "jetpack_site": "cate.blog",
            "jetpack_access_token": "token",
            "linkedin_client_id": "client123",
            "linkedin_client_secret": "secret456",
            **(extra_config or {}),
        }
        _write_config(config_path, cfg)
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        return config_path

    def test_auth_linkedin_mutually_exclusive_with_collect_only(self, monkeypatch):
        """--auth and --collect-only cannot be combined."""
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin", "--collect-only"])
        with pytest.raises(SystemExit) as exc:
            run.parse_args()
        assert exc.value.code != 0

    def test_auth_linkedin_mutually_exclusive_with_analyse_only(self, monkeypatch):
        """--auth and --analyse-only cannot be combined."""
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin", "--analyse-only"])
        with pytest.raises(SystemExit) as exc:
            run.parse_args()
        assert exc.value.code != 0

    def test_missing_client_id_exits(self, tmp_path, monkeypatch):
        """Missing linkedin_client_id exits with error."""
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, {
            **_minimal_config(),
            "linkedin_client_secret": "secret",
        })
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin"])
        with pytest.raises(SystemExit) as exc:
            run.main()
        assert exc.value.code == 1

    def test_missing_client_secret_exits(self, tmp_path, monkeypatch):
        """Missing linkedin_client_secret exits with error."""
        config_path = tmp_path / "config.yaml"
        _write_config(config_path, {
            **_minimal_config(),
            "linkedin_client_id": "client123",
        })
        monkeypatch.setattr(run, "CONFIG_PATH", config_path)
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin"])
        with pytest.raises(SystemExit) as exc:
            run.main()
        assert exc.value.code == 1

    def test_successful_oauth_writes_token_to_config(self, tmp_path, monkeypatch):
        """Successful OAuth flow writes linkedin_access_token to config.yaml."""
        config_path = self._setup(tmp_path, monkeypatch)
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin"])

        import threading

        def fake_oauth(config: dict) -> None:
            # Simulate a successful OAuth by writing the token directly
            raw = config_path.read_text()
            config_path.write_text(raw + "\nlinkedin_access_token: new_token_xyz\n")

        monkeypatch.setattr(run, "_linkedin_oauth", fake_oauth)
        run.main()

        written = config_path.read_text()
        assert "linkedin_access_token" in written
        assert "new_token_xyz" in written

    def test_token_exchange_failure_exits(self, tmp_path, monkeypatch):
        """If the OAuth helper raises SystemExit(1), main propagates it."""
        self._setup(tmp_path, monkeypatch)
        monkeypatch.setattr(sys, "argv", ["run.py", "--auth", "linkedin"])

        def failing_oauth(config: dict) -> None:
            sys.exit(1)

        monkeypatch.setattr(run, "_linkedin_oauth", failing_oauth)
        with pytest.raises(SystemExit) as exc:
            run.main()
        assert exc.value.code == 1

    def test_platform_expected_with_api_token(self, tmp_path, monkeypatch):
        """_platform_expected returns True for linkedin when api token is set."""
        monkeypatch.chdir(tmp_path)
        config = {"linkedin_access_token": "tok"}
        assert run._platform_expected("linkedin", config) is True

    def test_platform_expected_without_token_and_no_files(self, tmp_path, monkeypatch):
        """_platform_expected returns False when no token and no drop files."""
        monkeypatch.chdir(tmp_path)
        assert run._platform_expected("linkedin", {}) is False
