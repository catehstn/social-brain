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
