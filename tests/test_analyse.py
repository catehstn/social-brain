"""Tests for analyse.py — prompt building logic."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import analyse


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def make_preamble(tmp_path: Path) -> Path:
    """Write a minimal preamble template and return its path."""
    p = tmp_path / "preamble.txt"
    p.write_text("PREAMBLE period={period_window} id={period_id}\n")
    return p


def make_suffix(tmp_path: Path) -> Path:
    """Write a minimal suffix template with all placeholders and return its path."""
    p = tmp_path / "suffix.txt"
    p.write_text(
        "SUFFIX period={period_id} window={period_window} "
        "range={date_range} platforms={platforms_available} "
        "focus={primary_focus} goals={goals} pillars={pillars} "
        "upcoming={upcoming_section} data={data_json}"
    )
    return p


def make_dashboard(tmp_path: Path) -> Path:
    """Write a stub Dashboard.jsx and return its path."""
    p = tmp_path / "Dashboard.jsx"
    p.write_text("// stub dashboard\n")
    return p


def patch_templates(tmp_path: Path):
    """Context manager: patch analyse module paths to use tmp files."""
    preamble = make_preamble(tmp_path)
    suffix = make_suffix(tmp_path)
    dashboard = make_dashboard(tmp_path)
    return patch.multiple(
        analyse,
        PREAMBLE_PATH=preamble,
        SUFFIX_PATH=suffix,
        DASHBOARD_PATH=dashboard,
    )


# ---------------------------------------------------------------------------
# _strip_html
# ---------------------------------------------------------------------------

class TestStripHtml:
    def test_removes_tags(self):
        assert "<p>" not in analyse._strip_html("<p>hello</p>")

    def test_text_preserved(self):
        assert "hello" in analyse._strip_html("<p>hello</p>")

    def test_decodes_amp(self):
        assert "&" in analyse._strip_html("Tom &amp; Jerry")

    def test_decodes_lt_gt(self):
        result = analyse._strip_html("&lt;b&gt;bold&lt;/b&gt;")
        assert "<" in result and ">" in result

    def test_decodes_quot(self):
        assert '"' in analyse._strip_html("say &quot;hi&quot;")

    def test_decodes_apos(self):
        assert "'" in analyse._strip_html("it&#39;s")

    def test_decodes_nbsp(self):
        result = analyse._strip_html("a&nbsp;b")
        assert result == "a b"

    def test_decodes_smart_quotes(self):
        result = analyse._strip_html("&#8220;hello&#8221;")
        assert '"' in result

    def test_collapses_whitespace(self):
        result = analyse._strip_html("a   b\t\nc")
        assert "  " not in result

    def test_strips_outer_whitespace(self):
        result = analyse._strip_html("  hello  ")
        assert result == "hello"

    def test_empty_string(self):
        assert analyse._strip_html("") == ""

    def test_nested_tags(self):
        result = analyse._strip_html("<div><p><strong>hi</strong></p></div>")
        assert "hi" in result
        assert "<" not in result


# ---------------------------------------------------------------------------
# _trim_data
# ---------------------------------------------------------------------------

class TestTrimData:
    def _mastodon_post(self, **kwargs) -> dict:
        defaults = {
            "id": "123",
            "url": "https://example.com/123",
            "content": "<p>Hello</p>",
            "favourites": 0,
            "boosts": 0,
            "replies": 0,
        }
        return {**defaults, **kwargs}

    def _bluesky_post(self, **kwargs) -> dict:
        defaults = {
            "uri": "at://did/app.bsky.feed.post/abc",
            "text": "Hello",
            "likes": 0,
            "reposts": 0,
            "replies": 0,
        }
        return {**defaults, **kwargs}

    def test_does_not_mutate_original(self):
        original = {"mastodon": {"posts": [self._mastodon_post(content="<p>hi</p>")]}}
        analyse._trim_data(original)
        assert original["mastodon"]["posts"][0]["content"] == "<p>hi</p>"

    # Mastodon
    def test_mastodon_strips_html(self):
        data = {"mastodon": {"posts": [self._mastodon_post(content="<p>hello</p>")]}}
        result = analyse._trim_data(data)
        assert result["mastodon"]["posts"][0]["content"] == "hello"

    def test_mastodon_truncates_content(self):
        long = "x" * 300
        data = {"mastodon": {"posts": [self._mastodon_post(content=long)]}}
        result = analyse._trim_data(data)
        assert len(result["mastodon"]["posts"][0]["content"]) <= 200

    def test_mastodon_drops_id(self):
        data = {"mastodon": {"posts": [self._mastodon_post()]}}
        result = analyse._trim_data(data)
        assert "id" not in result["mastodon"]["posts"][0]

    def test_mastodon_drops_url(self):
        data = {"mastodon": {"posts": [self._mastodon_post()]}}
        result = analyse._trim_data(data)
        assert "url" not in result["mastodon"]["posts"][0]

    def test_mastodon_sorts_by_engagement(self):
        posts = [
            self._mastodon_post(favourites=1),
            self._mastodon_post(favourites=10),
            self._mastodon_post(favourites=5),
        ]
        data = {"mastodon": {"posts": posts}}
        result = analyse._trim_data(data)
        scores = [p["favourites"] for p in result["mastodon"]["posts"]]
        assert scores == sorted(scores, reverse=True)

    def test_mastodon_caps_at_15(self):
        posts = [self._mastodon_post(favourites=i) for i in range(50)]
        data = {"mastodon": {"posts": posts}}
        result = analyse._trim_data(data)
        assert len(result["mastodon"]["posts"]) == 15

    def test_mastodon_note_added(self):
        posts = [self._mastodon_post() for _ in range(5)]
        data = {"mastodon": {"posts": posts}}
        result = analyse._trim_data(data)
        assert "note" in result["mastodon"]

    def test_mastodon_none_content_does_not_crash(self):
        data = {"mastodon": {"posts": [self._mastodon_post(content=None)]}}
        result = analyse._trim_data(data)
        assert result["mastodon"]["posts"][0]["content"] == ""

    # Bluesky
    def test_bluesky_truncates_text(self):
        data = {"bluesky": {"posts": [self._bluesky_post(text="y" * 300)]}}
        result = analyse._trim_data(data)
        assert len(result["bluesky"]["posts"][0]["text"]) <= 200

    def test_bluesky_drops_uri(self):
        data = {"bluesky": {"posts": [self._bluesky_post()]}}
        result = analyse._trim_data(data)
        assert "uri" not in result["bluesky"]["posts"][0]

    def test_bluesky_sorts_by_engagement(self):
        posts = [
            self._bluesky_post(likes=3),
            self._bluesky_post(likes=10),
            self._bluesky_post(likes=1),
        ]
        data = {"bluesky": {"posts": posts}}
        result = analyse._trim_data(data)
        scores = [p["likes"] for p in result["bluesky"]["posts"]]
        assert scores == sorted(scores, reverse=True)

    def test_bluesky_caps_at_15(self):
        posts = [self._bluesky_post(likes=i) for i in range(50)]
        data = {"bluesky": {"posts": posts}}
        result = analyse._trim_data(data)
        assert len(result["bluesky"]["posts"]) == 15

    def test_bluesky_note_added(self):
        data = {"bluesky": {"posts": [self._bluesky_post()]}}
        result = analyse._trim_data(data)
        assert "note" in result["bluesky"]

    # Buttondown
    def test_buttondown_drops_body(self):
        data = {"buttondown": {"newsletters": [{"subject": "Hi", "body": "Long body", "id": "abc"}]}}
        result = analyse._trim_data(data)
        assert "body" not in result["buttondown"]["newsletters"][0]

    def test_buttondown_drops_id(self):
        data = {"buttondown": {"newsletters": [{"subject": "Hi", "body": "x", "id": "abc"}]}}
        result = analyse._trim_data(data)
        assert "id" not in result["buttondown"]["newsletters"][0]

    def test_buttondown_keeps_subject(self):
        data = {"buttondown": {"newsletters": [{"subject": "Hi", "body": "x", "id": "abc"}]}}
        result = analyse._trim_data(data)
        assert result["buttondown"]["newsletters"][0]["subject"] == "Hi"

    # Vercel
    def test_vercel_keeps_up_to_30_days(self):
        daily = [{"date": f"2025-01-{i:02d}", "views": i} for i in range(1, 25)]
        data = {"vercel": {"daily_views": daily}}
        result = analyse._trim_data(data)
        assert len(result["vercel"]["daily_views"]) == 24

    def test_vercel_caps_at_30_days(self):
        daily = [{"date": f"2025-01-{i:02d}", "views": i} for i in range(50)]
        data = {"vercel": {"daily_views": daily}}
        result = analyse._trim_data(data)
        assert len(result["vercel"]["daily_views"]) == 30

    def test_vercel_keeps_most_recent(self):
        daily = [{"date": str(i)} for i in range(50)]
        data = {"vercel": {"daily_views": daily}}
        result = analyse._trim_data(data)
        # Should be the last 30 entries
        assert result["vercel"]["daily_views"][0]["date"] == "20"
        assert result["vercel"]["daily_views"][-1]["date"] == "49"

    def test_vercel_note_added_when_capped(self):
        daily = [{"date": str(i)} for i in range(50)]
        data = {"vercel": {"daily_views": daily}}
        result = analyse._trim_data(data)
        assert "daily_views_note" in result["vercel"]

    def test_vercel_no_note_when_not_capped(self):
        daily = [{"date": str(i)} for i in range(10)]
        data = {"vercel": {"daily_views": daily}}
        result = analyse._trim_data(data)
        assert "daily_views_note" not in result["vercel"]

    # Upcoming / WordPress
    def test_upcoming_wordpress_truncates_content(self):
        data = {"upcoming": {"sources": {"wordpress": [
            {"title": "Post", "content": "x" * 1000}
        ]}}}
        result = analyse._trim_data(data)
        assert len(result["upcoming"]["sources"]["wordpress"][0]["content"]) <= 500

    def test_empty_data_no_crash(self):
        result = analyse._trim_data({})
        assert result == {}


# ---------------------------------------------------------------------------
# _trim_data — LinkedIn
# ---------------------------------------------------------------------------

class TestTrimDataLinkedIn:
    def _linkedin_post(self, text="Post text", impressions=100, engagements=10):
        return {"text": text, "impressions": impressions, "engagements": engagements}

    def _linkedin_data(self, num_posts=5, num_daily=10):
        return {
            "linkedin": {
                "top_posts_by_engagement": [self._linkedin_post() for _ in range(num_posts)],
                "top_posts_by_impressions": [self._linkedin_post() for _ in range(num_posts)],
                "daily_engagement": [{"date": f"2025-01-{i+1:02d}", "impressions": i} for i in range(num_daily)],
                "demographics": {"seniority": {"Senior": 0.4}},
            }
        }

    def test_drops_impressions_list(self):
        data = self._linkedin_data()
        result = analyse._trim_data(data)
        assert "top_posts_by_impressions" not in result["linkedin"]

    def test_adds_impressions_note(self):
        data = self._linkedin_data()
        result = analyse._trim_data(data)
        assert "top_posts_by_impressions_note" in result["linkedin"]

    def test_drops_demographics(self):
        data = self._linkedin_data()
        result = analyse._trim_data(data)
        assert "demographics" not in result["linkedin"]

    def test_caps_engagement_posts_at_15_weekly(self):
        data = self._linkedin_data(num_posts=50)
        result = analyse._trim_data(data)
        assert len(result["linkedin"]["top_posts_by_engagement"]) == 15

    def test_caps_engagement_posts_at_25_monthly(self):
        data = self._linkedin_data(num_posts=50)
        result = analyse._trim_data(data, months=3)
        assert len(result["linkedin"]["top_posts_by_engagement"]) == 25

    def test_truncates_post_text(self):
        long_text = "x" * 1000
        data = {"linkedin": {
            "top_posts_by_engagement": [self._linkedin_post(text=long_text)],
            "top_posts_by_impressions": [],
            "daily_engagement": [],
        }}
        result = analyse._trim_data(data)
        assert len(result["linkedin"]["top_posts_by_engagement"][0]["text"]) <= 300

    def test_drops_post_url(self):
        data = {"linkedin": {
            "top_posts_by_engagement": [{"text": "hi", "url": "https://linkedin.com/post/123"}],
            "top_posts_by_impressions": [],
            "daily_engagement": [],
        }}
        result = analyse._trim_data(data)
        assert "url" not in result["linkedin"]["top_posts_by_engagement"][0]

    def test_caps_daily_engagement_at_30(self):
        data = self._linkedin_data(num_daily=60)
        result = analyse._trim_data(data)
        assert len(result["linkedin"]["daily_engagement"]) == 30

    def test_daily_engagement_note_added_when_capped(self):
        data = self._linkedin_data(num_daily=60)
        result = analyse._trim_data(data)
        assert "daily_engagement_note" in result["linkedin"]

    def test_daily_engagement_keeps_most_recent(self):
        days = [{"date": f"day-{i}", "impressions": i} for i in range(60)]
        data = {"linkedin": {
            "top_posts_by_engagement": [],
            "top_posts_by_impressions": [],
            "daily_engagement": days,
        }}
        result = analyse._trim_data(data)
        kept = result["linkedin"]["daily_engagement"]
        # Should keep the last 30 (most recent)
        assert kept[0]["date"] == "day-30"
        assert kept[-1]["date"] == "day-59"

    def test_no_crash_when_linkedin_missing_keys(self):
        data = {"linkedin": {}}
        result = analyse._trim_data(data)
        assert "linkedin" in result

    def test_none_post_text_does_not_crash(self):
        data = {"linkedin": {
            "top_posts_by_engagement": [{"text": None, "impressions": 10, "engagements": 1}],
            "top_posts_by_impressions": [],
            "daily_engagement": [],
        }}
        result = analyse._trim_data(data)
        assert result["linkedin"]["top_posts_by_engagement"][0]["text"] == ""


# ---------------------------------------------------------------------------
# _format_upcoming_section
# ---------------------------------------------------------------------------

class TestFormatUpcomingSection:
    def test_empty_data_returns_empty(self):
        assert analyse._format_upcoming_section({}) == ""

    def test_upcoming_key_missing_returns_empty(self):
        assert analyse._format_upcoming_section({"mastodon": {}}) == ""

    def test_only_header_no_sources_returns_empty(self):
        data = {"upcoming": {"sources": {}}}
        assert analyse._format_upcoming_section(data) == ""

    def test_wordpress_post_formatted(self):
        data = {"upcoming": {"sources": {"wordpress": [
            {"title": "<b>My Post</b>", "scheduled_date": "2025-06-01T09:00:00"}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert "WordPress" in result
        assert "2025-06-01" in result
        assert "My Post" in result
        assert "<b>" not in result  # HTML stripped

    def test_buttondown_email_formatted(self):
        data = {"upcoming": {"sources": {"buttondown": [
            {"subject": "Weekly digest", "scheduled_date": "2025-06-02T10:00:00"}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert "Buttondown" in result
        assert "2025-06-02" in result
        assert "Weekly digest" in result

    def test_buffer_post_formatted(self):
        data = {"upcoming": {"sources": {"buffer": [
            {"platform": "twitter", "scheduled_at": "2025-06-03T12:00:00", "text": "Hello world"}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert "Buffer" in result
        assert "twitter" in result
        assert "Hello world" in result

    def test_buffer_long_text_truncated(self):
        long_text = "x" * 200
        data = {"upcoming": {"sources": {"buffer": [
            {"platform": "twitter", "scheduled_at": "2025-06-03T12:00:00", "text": long_text}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert "…" in result

    def test_buffer_short_text_no_ellipsis(self):
        data = {"upcoming": {"sources": {"buffer": [
            {"platform": "twitter", "scheduled_at": "2025-06-03T12:00:00", "text": "short"}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert "…" not in result

    def test_section_ends_with_double_newline(self):
        data = {"upcoming": {"sources": {"wordpress": [
            {"title": "T", "scheduled_date": "2025-06-01"}
        ]}}}
        result = analyse._format_upcoming_section(data)
        assert result.endswith("\n\n")

    def test_multiple_sources_all_present(self):
        data = {"upcoming": {"sources": {
            "wordpress": [{"title": "Blog", "scheduled_date": "2025-06-01"}],
            "buttondown": [{"subject": "News", "scheduled_date": "2025-06-02"}],
        }}}
        result = analyse._format_upcoming_section(data)
        assert "WordPress" in result
        assert "Buttondown" in result


# ---------------------------------------------------------------------------
# build_prompt
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    def _base_config(self) -> dict:
        return {
            "primary_focus": "newsletter growth",
            "content_pillars": ["Tech", "Writing"],
            "weekly_goals": ["Grow subs", "Publish post"],
        }

    def _base_data(self) -> dict:
        return {
            "mastodon": {
                "collected_at": "2025-06-10T12:00:00Z",
                "since": "2025-05-27T00:00:00Z",
                "posts": [],
            }
        }

    def test_returns_non_empty_string(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_weekly_period_window(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23", months=None)
        assert "Weekly" in result

    def test_monthly_period_window(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23-3m", months=3)
        assert "3-Month" in result

    def test_period_id_in_prompt(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert "2025-W23" in result

    def test_date_range_from_data(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt(self._base_data(), self._base_config(), "2025-W23")
        # collected_at is 2025-06-10, since is 2025-05-27
        assert "2025" in result
        assert "Jun" in result

    def test_date_range_fallback_when_no_data(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        # Should contain today's date or some date string
        assert len(result) > 0  # at minimum doesn't crash

    def test_platforms_available_listed(self, tmp_path):
        data = {
            "mastodon": {"collected_at": "2025-06-10", "posts": []},
            "bluesky": {"collected_at": "2025-06-10", "posts": []},
        }
        with patch_templates(tmp_path):
            result = analyse.build_prompt(data, self._base_config(), "2025-W23")
        assert "mastodon" in result
        assert "bluesky" in result

    def test_upcoming_excluded_from_platforms(self, tmp_path):
        # "upcoming" should not appear as a listed platform alongside real platforms
        data = {
            "mastodon": {"collected_at": "2025-06-10", "posts": []},
            "upcoming": {"sources": {}},
        }
        with patch_templates(tmp_path):
            result = analyse.build_prompt(data, self._base_config(), "2025-W23")
        assert "- mastodon" in result
        assert "- upcoming" not in result

    def test_primary_focus_in_prompt(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert "newsletter growth" in result

    def test_content_pillars_in_prompt(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert "Tech" in result
        assert "Writing" in result

    def test_weekly_goals_in_prompt(self, tmp_path):
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert "Grow subs" in result

    def test_data_json_embedded(self, tmp_path):
        data = {"mastodon": {"collected_at": "2025-06-10", "posts": [], "followers": 42}}
        with patch_templates(tmp_path):
            result = analyse.build_prompt(data, self._base_config(), "2025-W23")
        assert "42" in result  # followers value appears in JSON

    def test_missing_preamble_file_no_crash(self, tmp_path):
        suffix = make_suffix(tmp_path)
        dashboard = make_dashboard(tmp_path)
        missing = tmp_path / "nonexistent_preamble.txt"
        with patch.multiple(analyse, PREAMBLE_PATH=missing, SUFFIX_PATH=suffix, DASHBOARD_PATH=dashboard):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert isinstance(result, str)

    def test_missing_suffix_file_no_crash(self, tmp_path):
        preamble = make_preamble(tmp_path)
        dashboard = make_dashboard(tmp_path)
        missing = tmp_path / "nonexistent_suffix.txt"
        with patch.multiple(analyse, PREAMBLE_PATH=preamble, SUFFIX_PATH=missing, DASHBOARD_PATH=dashboard):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert isinstance(result, str)

    def test_missing_dashboard_no_crash(self, tmp_path):
        preamble = make_preamble(tmp_path)
        suffix = make_suffix(tmp_path)
        missing = tmp_path / "nonexistent_Dashboard.jsx"
        with patch.multiple(analyse, PREAMBLE_PATH=preamble, SUFFIX_PATH=suffix, DASHBOARD_PATH=missing):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert isinstance(result, str)

    def test_empty_pillars_uses_placeholder(self, tmp_path):
        config = {**self._base_config(), "content_pillars": []}
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, config, "2025-W23")
        assert "(none specified)" in result

    def test_empty_goals_uses_placeholder(self, tmp_path):
        config = {**self._base_config(), "weekly_goals": []}
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, config, "2025-W23")
        assert "(none specified)" in result

    def test_dashboard_reference_in_preamble(self):
        # preamble.txt must include the "Reference Dashboard.jsx:" label that
        # introduces the embedded dashboard content (not rely on a URL fetch)
        preamble = analyse.PREAMBLE_PATH.read_text()
        assert "Reference Dashboard.jsx" in preamble

    def test_dashboard_content_embedded_in_prompt(self, tmp_path):
        # build_prompt must embed the dashboard content directly
        with patch_templates(tmp_path):
            result = analyse.build_prompt({}, self._base_config(), "2025-W23")
        assert "// stub dashboard" in result


# ---------------------------------------------------------------------------
# save_prompt
# ---------------------------------------------------------------------------

class TestSavePrompt:
    def test_creates_reports_dir(self, tmp_path):
        reports_dir = tmp_path / "reports"
        assert not reports_dir.exists()
        with patch_templates(tmp_path):
            analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert reports_dir.exists()

    def test_writes_prompt_file(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert path.exists()

    def test_filename_includes_period(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert "2025-W23" in path.name

    def test_filename_pattern(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert path.name == "prompt-2025-W23.txt"

    def test_returns_correct_path(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert path == reports_dir / "prompt-2025-W23.txt"

    def test_file_content_is_non_empty(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23", reports_dir)
        assert len(path.read_text()) > 0

    def test_months_param_passed_through(self, tmp_path):
        reports_dir = tmp_path / "reports"
        with patch_templates(tmp_path):
            path = analyse.save_prompt({}, {}, "2025-W23-3m", reports_dir, months=3)
        content = path.read_text()
        assert "3-Month" in content
