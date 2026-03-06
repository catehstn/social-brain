"""
Tests for store.py — all file I/O uses tmp_path, no real config required.
"""
from __future__ import annotations

import pandas as pd
import pytest

from store import (
    _load,
    _upsert,
    get_known_platforms,
    update,
    _process_mastodon,
    _process_bluesky,
    _process_jetpack,
    _process_linkedin,
    _process_buttondown,
    _process_vercel,
    _process_amazon,
    _process_mentions,
)

NOW = "2026-03-06 12:00:00"


# ---------------------------------------------------------------------------
# _upsert
# ---------------------------------------------------------------------------

class TestUpsert:
    def test_new_rows_into_empty(self):
        existing = pd.DataFrame()
        new = pd.DataFrame([{"id": "1", "val": "a"}])
        result = _upsert(existing, new, ["id"])
        assert len(result) == 1
        assert result.iloc[0]["val"] == "a"

    def test_appends_new_key(self):
        existing = pd.DataFrame([{"id": "1", "val": "a"}])
        new = pd.DataFrame([{"id": "2", "val": "b"}])
        result = _upsert(existing, new, ["id"])
        assert len(result) == 2

    def test_replaces_existing_key(self):
        existing = pd.DataFrame([{"id": "1", "val": "old"}])
        new = pd.DataFrame([{"id": "1", "val": "new"}])
        result = _upsert(existing, new, ["id"])
        assert len(result) == 1
        assert result.iloc[0]["val"] == "new"

    def test_partial_replacement(self):
        existing = pd.DataFrame([
            {"id": "1", "val": "keep"},
            {"id": "2", "val": "old"},
        ])
        new = pd.DataFrame([{"id": "2", "val": "updated"}])
        result = _upsert(existing, new, ["id"])
        assert len(result) == 2
        assert result[result["id"] == "1"].iloc[0]["val"] == "keep"
        assert result[result["id"] == "2"].iloc[0]["val"] == "updated"

    def test_empty_new_returns_existing(self):
        existing = pd.DataFrame([{"id": "1", "val": "a"}])
        new = pd.DataFrame()
        result = _upsert(existing, new, ["id"])
        assert len(result) == 1

    def test_composite_key(self):
        existing = pd.DataFrame([{"site": "a.com", "query": "foo", "clicks": 5}])
        new = pd.DataFrame([{"site": "a.com", "query": "foo", "clicks": 10}])
        result = _upsert(existing, new, ["site", "query"])
        assert len(result) == 1
        assert int(result.iloc[0]["clicks"]) == 10

    def test_missing_key_column_in_existing(self):
        # key column not yet present in existing — should concat safely
        existing = pd.DataFrame([{"other": "x"}])
        new = pd.DataFrame([{"id": "1", "val": "a"}])
        result = _upsert(existing, new, ["id"])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _load
# ---------------------------------------------------------------------------

class TestLoad:
    def test_missing_file_returns_empty(self, tmp_path):
        result = _load(tmp_path / "nofile.xlsx", "sheet1")
        assert result.empty

    def test_missing_sheet_returns_empty(self, tmp_path):
        path = tmp_path / "store.xlsx"
        df = pd.DataFrame([{"a": 1}])
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="other", index=False)
        result = _load(path, "missing_sheet")
        assert result.empty

    def test_loads_existing_sheet(self, tmp_path):
        path = tmp_path / "store.xlsx"
        df = pd.DataFrame([{"post_id": "abc", "val": "42"}])
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="mastodon_posts", index=False)
        result = _load(path, "mastodon_posts")
        assert len(result) == 1
        assert result.iloc[0]["post_id"] == "abc"


# ---------------------------------------------------------------------------
# get_known_platforms
# ---------------------------------------------------------------------------

class TestGetKnownPlatforms:
    def test_missing_file_returns_empty_set(self, tmp_path):
        result = get_known_platforms(tmp_path / "nofile.xlsx")
        assert result == set()

    def test_recognises_populated_sheets(self, tmp_path):
        path = tmp_path / "store.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            pd.DataFrame([{"post_id": "1"}]).to_excel(w, sheet_name="mastodon_posts", index=False)
            pd.DataFrame([{"post_uri": "at://x"}]).to_excel(w, sheet_name="bluesky_posts", index=False)
        result = get_known_platforms(path)
        assert "mastodon" in result
        assert "bluesky" in result

    def test_empty_sheet_not_counted(self, tmp_path):
        path = tmp_path / "store.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            pd.DataFrame().to_excel(w, sheet_name="mastodon_posts", index=False)
        result = get_known_platforms(path)
        assert "mastodon" not in result

    def test_corrupt_file_returns_empty_set(self, tmp_path):
        path = tmp_path / "store.xlsx"
        path.write_bytes(b"not an excel file")
        result = get_known_platforms(path)
        assert result == set()


# ---------------------------------------------------------------------------
# _process_mastodon
# ---------------------------------------------------------------------------

class TestProcessMastodon:
    def _collected(self, **overrides):
        base = {
            "handle": "cate@hachyderm.io",
            "posts": [
                {"id": "p1", "created_at": "2026-03-01T10:00:00Z", "content": "<p>Hello</p>",
                 "favourites": 5, "boosts": 2, "replies": 1, "has_attachment": False},
            ],
            "account": {"followers": 1234},
            "new_follows": [],
        }
        base.update(overrides)
        return base

    def test_writes_posts_sheet(self, tmp_path):
        sheets = {}
        _process_mastodon(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "mastodon_posts" in sheets
        assert len(sheets["mastodon_posts"]) == 1

    def test_post_columns(self, tmp_path):
        sheets = {}
        _process_mastodon(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        row = sheets["mastodon_posts"].iloc[0]
        assert row["post_id"] == "p1"
        assert row["handle"] == "cate@hachyderm.io"
        assert int(row["favourites"]) == 5

    def test_content_truncated_to_500(self, tmp_path):
        long_content = "x" * 600
        collected = self._collected(posts=[
            {"id": "p2", "created_at": "2026-03-01T10:00:00Z", "content": long_content,
             "favourites": 0, "boosts": 0, "replies": 0, "has_attachment": False},
        ])
        sheets = {}
        _process_mastodon(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert len(sheets["mastodon_posts"].iloc[0]["content"]) <= 500

    def test_writes_account_snapshot(self, tmp_path):
        sheets = {}
        _process_mastodon(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "account_snapshots" in sheets
        snap = sheets["account_snapshots"]
        assert int(snap.iloc[0]["value"]) == 1234
        assert snap.iloc[0]["platform"] == "mastodon"
        assert snap.iloc[0]["metric"] == "followers"

    def test_writes_follows(self, tmp_path):
        collected = self._collected(new_follows=[
            {"followed_at": "2026-03-01T09:00:00Z", "account": "someone@hachyderm.io",
             "display_name": "Someone", "followers": 50},
        ])
        sheets = {}
        _process_mastodon(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "follows" in sheets
        assert sheets["follows"].iloc[0]["account"] == "someone@hachyderm.io"

    def test_no_posts_no_posts_sheet(self, tmp_path):
        collected = self._collected(posts=[])
        sheets = {}
        _process_mastodon(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "mastodon_posts" not in sheets

    def test_post_without_id_skipped(self, tmp_path):
        collected = self._collected(posts=[
            {"created_at": "2026-03-01T10:00:00Z", "content": "no id", "favourites": 0, "boosts": 0, "replies": 0},
        ])
        sheets = {}
        _process_mastodon(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "mastodon_posts" not in sheets

    def test_idempotent(self, tmp_path):
        path = tmp_path / "s.xlsx"
        for _ in range(2):
            sheets = {}
            _process_mastodon(self._collected(), sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "mastodon_posts")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _process_bluesky
# ---------------------------------------------------------------------------

class TestProcessBluesky:
    def _collected(self, **overrides):
        base = {
            "handle": "catehstn.bsky.social",
            "posts": [
                {"uri": "at://did:plc:abc/app.bsky.feed.post/1", "created_at": "2026-03-01T10:00:00Z",
                 "text": "Hello Bluesky", "likes": 10, "reposts": 3, "replies": 2, "has_attachment": False},
            ],
            "new_follows": [],
        }
        base.update(overrides)
        return base

    def test_writes_posts_sheet(self, tmp_path):
        sheets = {}
        _process_bluesky(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "bluesky_posts" in sheets
        assert len(sheets["bluesky_posts"]) == 1

    def test_post_columns(self, tmp_path):
        sheets = {}
        _process_bluesky(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        row = sheets["bluesky_posts"].iloc[0]
        assert row["post_uri"].startswith("at://")
        assert int(row["likes"]) == 10

    def test_post_without_uri_skipped(self, tmp_path):
        collected = self._collected(posts=[{"text": "no uri", "likes": 0, "reposts": 0, "replies": 0}])
        sheets = {}
        _process_bluesky(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "bluesky_posts" not in sheets

    def test_writes_follows(self, tmp_path):
        collected = self._collected(new_follows=[
            {"followed_at": "2026-03-01T09:00:00Z", "handle": "other.bsky.social",
             "display_name": "Other", "followers": 100},
        ])
        sheets = {}
        _process_bluesky(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "follows" in sheets
        assert sheets["follows"].iloc[0]["platform"] == "bluesky"

    def test_idempotent(self, tmp_path):
        path = tmp_path / "s.xlsx"
        for _ in range(2):
            sheets = {}
            _process_bluesky(self._collected(), sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "bluesky_posts")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _process_jetpack
# ---------------------------------------------------------------------------

class TestProcessJetpack:
    def _collected(self):
        return {
            "daily_views": [
                {"date": "2026-03-01", "views": 100},
                {"date": "2026-03-02", "views": 120},
            ],
            "top_posts": [
                {"href": "https://cate.blog/post-a", "title": "Post A", "views": 80},
            ],
            "referrers": [
                {"name": "twitter.com", "views": 30},
            ],
        }

    def test_writes_three_sheets(self, tmp_path):
        sheets = {}
        _process_jetpack(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "jetpack_daily" in sheets
        assert "jetpack_top_posts" in sheets
        assert "jetpack_referrers" in sheets

    def test_daily_row_count(self, tmp_path):
        sheets = {}
        _process_jetpack(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert len(sheets["jetpack_daily"]) == 2

    def test_daily_upsert_updates_views(self, tmp_path):
        path = tmp_path / "s.xlsx"
        first = {"daily_views": [{"date": "2026-03-01", "views": 100}], "top_posts": [], "referrers": []}
        second = {"daily_views": [{"date": "2026-03-01", "views": 150}], "top_posts": [], "referrers": []}
        for collected in [first, second]:
            sheets = {}
            _process_jetpack(collected, sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "jetpack_daily")
        assert len(result) == 1
        assert int(result.iloc[0]["views"]) == 150

    def test_entry_without_date_skipped(self, tmp_path):
        collected = {"daily_views": [{"views": 50}], "top_posts": [], "referrers": []}
        sheets = {}
        _process_jetpack(collected, sheets, tmp_path / "s.xlsx", NOW)
        # Sheet may be written but must be empty (date filter removed the row)
        assert "jetpack_daily" not in sheets or sheets["jetpack_daily"].empty

    def test_top_post_without_href_skipped(self, tmp_path):
        collected = {"daily_views": [], "top_posts": [{"title": "No URL", "views": 10}], "referrers": []}
        sheets = {}
        _process_jetpack(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "jetpack_top_posts" not in sheets or sheets["jetpack_top_posts"].empty


# ---------------------------------------------------------------------------
# _process_linkedin
# ---------------------------------------------------------------------------

class TestProcessLinkedin:
    def _collected(self):
        return {
            "daily_engagement": [
                {"date": "2026-03-01", "impressions": 500, "engagements": 20, "new_followers": 3},
            ],
            "top_posts_by_engagement": [
                {"url": "https://linkedin.com/post/1", "date": "2026-03-01", "engagements": 15, "text": "Post 1"},
            ],
            "top_posts_by_impressions": [
                {"url": "https://linkedin.com/post/1", "date": "2026-03-01", "impressions": 400, "text": "Post 1"},
                {"url": "https://linkedin.com/post/2", "date": "2026-03-01", "impressions": 200, "text": "Post 2"},
            ],
        }

    def test_writes_daily_sheet(self, tmp_path):
        sheets = {}
        _process_linkedin(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "linkedin_daily" in sheets
        assert int(sheets["linkedin_daily"].iloc[0]["impressions"]) == 500

    def test_merges_engagement_and_impressions_by_url(self, tmp_path):
        sheets = {}
        _process_linkedin(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        posts = sheets["linkedin_posts"]
        # post/1 appears in both lists — should be one row with both values
        p1 = posts[posts["url"] == "https://linkedin.com/post/1"].iloc[0]
        assert p1["engagements"] is not None
        assert p1["impressions"] is not None
        # post/2 only in impressions — engagements should be NaN/None
        p2 = posts[posts["url"] == "https://linkedin.com/post/2"].iloc[0]
        assert pd.isna(p2["engagements"])

    def test_total_post_rows(self, tmp_path):
        sheets = {}
        _process_linkedin(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert len(sheets["linkedin_posts"]) == 2

    def test_daily_entry_without_date_skipped(self, tmp_path):
        collected = {
            "daily_engagement": [{"impressions": 100, "engagements": 5, "new_followers": 0}],
            "top_posts_by_engagement": [],
            "top_posts_by_impressions": [],
        }
        sheets = {}
        _process_linkedin(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "linkedin_daily" not in sheets or sheets["linkedin_daily"].empty


# ---------------------------------------------------------------------------
# _process_buttondown
# ---------------------------------------------------------------------------

class TestProcessButtondown:
    def _collected(self):
        return {
            "newsletters": [
                {"id": "e1", "newsletter": "eng-mgmt", "subject": "Issue 1",
                 "send_date": "2026-03-01", "recipients": 500, "opens": 150, "clicks": 30,
                 "open_rate": 0.30, "click_rate": 0.06, "unsubscribes": 2, "new_subscribers": 10},
            ],
            "subscriber_counts": {"eng-mgmt": 520},
        }

    def test_writes_emails_sheet(self, tmp_path):
        sheets = {}
        _process_buttondown(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "buttondown_emails" in sheets
        assert len(sheets["buttondown_emails"]) == 1

    def test_email_columns(self, tmp_path):
        sheets = {}
        _process_buttondown(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        row = sheets["buttondown_emails"].iloc[0]
        assert row["email_id"] == "e1"
        assert int(row["recipients"]) == 500

    def test_writes_subscribers_sheet(self, tmp_path):
        sheets = {}
        _process_buttondown(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "buttondown_subscribers" in sheets
        sub = sheets["buttondown_subscribers"].iloc[0]
        assert sub["newsletter"] == "eng-mgmt"
        assert int(sub["subscribers"]) == 520

    def test_email_without_id_skipped(self, tmp_path):
        collected = {
            "newsletters": [{"newsletter": "x", "subject": "No ID", "recipients": 0}],
            "subscriber_counts": {},
        }
        sheets = {}
        _process_buttondown(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "buttondown_emails" not in sheets or sheets["buttondown_emails"].empty

    def test_idempotent(self, tmp_path):
        path = tmp_path / "s.xlsx"
        for _ in range(2):
            sheets = {}
            _process_buttondown(self._collected(), sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "buttondown_emails")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _process_vercel
# ---------------------------------------------------------------------------

class TestProcessVercel:
    def _collected(self):
        return {
            "daily": [
                {"date": "2026-03-01", "page_views": 200, "visitors": 150},
                {"date": "2026-03-02", "page_views": 180, "visitors": 130},
            ],
        }

    def test_writes_daily_sheet(self, tmp_path):
        sheets = {}
        _process_vercel(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "vercel_daily" in sheets
        assert len(sheets["vercel_daily"]) == 2

    def test_upserts_by_date(self, tmp_path):
        path = tmp_path / "s.xlsx"
        first = {"daily": [{"date": "2026-03-01", "page_views": 100, "visitors": 80}]}
        second = {"daily": [{"date": "2026-03-01", "page_views": 200, "visitors": 160}]}
        for collected in [first, second]:
            sheets = {}
            _process_vercel(collected, sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "vercel_daily")
        assert len(result) == 1
        assert int(result.iloc[0]["page_views"]) == 200

    def test_entry_without_date_skipped(self, tmp_path):
        collected = {"daily": [{"page_views": 100, "visitors": 80}]}
        sheets = {}
        _process_vercel(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "vercel_daily" not in sheets or sheets["vercel_daily"].empty


# ---------------------------------------------------------------------------
# _process_amazon
# ---------------------------------------------------------------------------

class TestProcessAmazon:
    def _collected(self):
        return {
            "by_marketplace": {
                "amazon.com": [
                    {"asin": "B0CW1MYCGK", "title": "My Book (Kindle)", "best_sellers_rank": 1234,
                     "rating": 4.5, "reviews": 100},
                ],
                "amazon.co.uk": [
                    {"asin": "B0CW1MYCGK", "title": "My Book (Kindle)", "best_sellers_rank": 5678,
                     "rating": 4.4, "reviews": 20},
                ],
            }
        }

    def test_writes_amazon_sheet(self, tmp_path):
        sheets = {}
        _process_amazon(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "amazon" in sheets
        assert len(sheets["amazon"]) == 2

    def test_composite_key_asin_marketplace(self, tmp_path):
        path = tmp_path / "s.xlsx"
        first = {"by_marketplace": {"amazon.com": [
            {"asin": "B0CW1MYCGK", "title": "Book", "best_sellers_rank": 1000, "rating": 4.5, "reviews": 50}
        ]}}
        second = {"by_marketplace": {"amazon.com": [
            {"asin": "B0CW1MYCGK", "title": "Book", "best_sellers_rank": 900, "rating": 4.5, "reviews": 55}
        ]}}
        for collected in [first, second]:
            sheets = {}
            _process_amazon(collected, sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "amazon")
        assert len(result) == 1
        assert int(result.iloc[0]["rank"]) == 900

    def test_empty_marketplaces_no_sheet(self, tmp_path):
        sheets = {}
        _process_amazon({"by_marketplace": {}}, sheets, tmp_path / "s.xlsx", NOW)
        assert "amazon" not in sheets


# ---------------------------------------------------------------------------
# _process_mentions
# ---------------------------------------------------------------------------

class TestProcessMentions:
    def _collected(self):
        return {
            "sources": {
                "hacker_news": [
                    {"objectID": "hn1", "type": "story", "title": "Cool post",
                     "url": "https://cate.blog/post", "points": 42, "num_comments": 5,
                     "created_at": "2026-03-01T10:00:00Z", "domain": "cate.blog"},
                ],
                "mastodon": [
                    {"id": "m1", "account": {"acct": "friend@mastodon.social"},
                     "status": {"content": "<p>Great post!</p>"}, "created_at": "2026-03-01T11:00:00Z"},
                ],
                "bluesky": [
                    {"uri": "at://did:plc:xyz/app.bsky.feed.post/1",
                     "author": {"handle": "friend.bsky.social"},
                     "record": {"text": "Nice work!"}, "indexedAt": "2026-03-01T12:00:00Z"},
                ],
                "google_search_console": [
                    {"site": "cate.blog", "query": "engineering management", "page": "https://cate.blog/post",
                     "clicks": 10, "impressions": 200, "ctr": 0.05, "position": 8.2},
                    {"site": "whatsmyjob.club", "query": "job titles", "page": "https://whatsmyjob.club/",
                     "clicks": 5, "impressions": 100, "ctr": 0.05, "position": 12.0},
                ],
            }
        }

    def test_writes_hn_sheet(self, tmp_path):
        sheets = {}
        _process_mentions(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "hn_mentions" in sheets
        assert len(sheets["hn_mentions"]) == 1

    def test_writes_mastodon_mentions_sheet(self, tmp_path):
        sheets = {}
        _process_mentions(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "mastodon_mentions" in sheets
        row = sheets["mastodon_mentions"].iloc[0]
        assert row["notification_id"] == "m1"
        assert row["account"] == "friend@mastodon.social"

    def test_writes_bluesky_mentions_sheet(self, tmp_path):
        sheets = {}
        _process_mentions(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "bluesky_mentions" in sheets
        row = sheets["bluesky_mentions"].iloc[0]
        assert row["author"] == "friend.bsky.social"

    def test_writes_gsc_sheet(self, tmp_path):
        sheets = {}
        _process_mentions(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        assert "gsc_queries" in sheets
        assert len(sheets["gsc_queries"]) == 2

    def test_gsc_includes_all_monitored_domains(self, tmp_path):
        sheets = {}
        _process_mentions(self._collected(), sheets, tmp_path / "s.xlsx", NOW)
        sites = set(sheets["gsc_queries"]["site"].tolist())
        assert "cate.blog" in sites
        assert "whatsmyjob.club" in sites

    def test_gsc_upsert_by_site_query_page(self, tmp_path):
        path = tmp_path / "s.xlsx"
        first = {"sources": {"google_search_console": [
            {"site": "cate.blog", "query": "mgmt", "page": "https://cate.blog/p",
             "clicks": 5, "impressions": 100, "ctr": 0.05, "position": 10.0},
        ]}}
        second = {"sources": {"google_search_console": [
            {"site": "cate.blog", "query": "mgmt", "page": "https://cate.blog/p",
             "clicks": 8, "impressions": 120, "ctr": 0.067, "position": 9.5},
        ]}}
        for collected in [first, second]:
            sheets = {}
            _process_mentions(collected, sheets, path, NOW)
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                for name, df in sheets.items():
                    df.to_excel(w, sheet_name=name, index=False)
        result = _load(path, "gsc_queries")
        assert len(result) == 1
        assert int(result.iloc[0]["clicks"]) == 8

    def test_empty_sources_writes_nothing(self, tmp_path):
        sheets = {}
        _process_mentions({"sources": {}}, sheets, tmp_path / "s.xlsx", NOW)
        assert not sheets

    def test_mastodon_mention_without_id_skipped(self, tmp_path):
        collected = {"sources": {"mastodon": [
            {"account": {"acct": "x@m.social"}, "status": {"content": "hi"}, "created_at": "2026-03-01T00:00:00Z"}
        ]}}
        sheets = {}
        _process_mentions(collected, sheets, tmp_path / "s.xlsx", NOW)
        assert "mastodon_mentions" not in sheets or sheets["mastodon_mentions"].empty


# ---------------------------------------------------------------------------
# update() — full pipeline
# ---------------------------------------------------------------------------

class TestUpdate:
    def test_creates_file_and_sheets(self, tmp_path):
        path = tmp_path / "analytics.xlsx"
        collected = {
            "mastodon": {
                "handle": "cate@hachyderm.io",
                "posts": [{"id": "p1", "created_at": "2026-03-01T10:00:00Z", "content": "Hi",
                           "favourites": 1, "boosts": 0, "replies": 0, "has_attachment": False}],
                "account": {"followers": 100},
                "new_follows": [],
            },
            "jetpack": {
                "daily_views": [{"date": "2026-03-01", "views": 50}],
                "top_posts": [],
                "referrers": [],
            },
        }
        update(collected, store_path=path)
        assert path.exists()
        xl = pd.ExcelFile(path)
        assert "mastodon_posts" in xl.sheet_names
        assert "jetpack_daily" in xl.sheet_names

    def test_preserves_untouched_sheets(self, tmp_path):
        path = tmp_path / "analytics.xlsx"
        # Write an initial sheet manually
        with pd.ExcelWriter(path, engine="openpyxl") as w:
            pd.DataFrame([{"foo": "bar"}]).to_excel(w, sheet_name="custom_sheet", index=False)

        collected = {
            "mastodon": {
                "handle": "cate@hachyderm.io",
                "posts": [{"id": "p1", "created_at": "2026-03-01T10:00:00Z", "content": "Hi",
                           "favourites": 1, "boosts": 0, "replies": 0, "has_attachment": False}],
                "account": {"followers": 100},
                "new_follows": [],
            }
        }
        update(collected, store_path=path)
        xl = pd.ExcelFile(path)
        assert "custom_sheet" in xl.sheet_names
        assert "mastodon_posts" in xl.sheet_names

    def test_nothing_to_write_skips_file_creation(self, tmp_path):
        path = tmp_path / "analytics.xlsx"
        update({}, store_path=path)
        assert not path.exists()

    def test_unknown_platform_in_collected_ignored(self, tmp_path):
        path = tmp_path / "analytics.xlsx"
        update({"upcoming": {"sources": {}}}, store_path=path)
        assert not path.exists()

    def test_processor_exception_does_not_crash_others(self, tmp_path, monkeypatch):
        path = tmp_path / "analytics.xlsx"

        def bad_processor(collected, sheets, store_path, now):
            raise RuntimeError("simulated failure")

        import store as store_module
        monkeypatch.setitem(store_module._PROCESSORS, "mastodon", bad_processor)

        collected = {
            "mastodon": {"posts": [], "account": {}, "new_follows": []},
            "jetpack": {"daily_views": [{"date": "2026-03-01", "views": 50}], "top_posts": [], "referrers": []},
        }
        update(collected, store_path=path)
        assert path.exists()
        xl = pd.ExcelFile(path)
        assert "jetpack_daily" in xl.sheet_names
        assert "mastodon_posts" not in xl.sheet_names

    def test_account_snapshots_accumulates_across_platforms(self, tmp_path):
        path = tmp_path / "analytics.xlsx"
        mastodon_collected = {
            "mastodon": {
                "handle": "cate@hachyderm.io",
                "posts": [],
                "account": {"followers": 1000},
                "new_follows": [],
            }
        }
        update(mastodon_collected, store_path=path)
        result = _load(path, "account_snapshots")
        assert len(result) == 1
        assert result.iloc[0]["platform"] == "mastodon"
