"""
Tests for collect.py — all HTTP calls mocked with respx, no real config needed.
"""
from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from collect import (
    collect_amazon,
    collect_bluesky,
    collect_buttondown,
    collect_calendly,
    collect_goatcounter,
    collect_jetpack,
    collect_linkedin,
    collect_mastodon,
    collect_mentions,
    collect_vercel,
    collect_all,
)

SINCE = datetime(2026, 2, 20, tzinfo=timezone.utc)
RECENT = "2026-03-01T10:00:00Z"
OLD = "2026-01-01T10:00:00Z"


# ---------------------------------------------------------------------------
# Mastodon
# ---------------------------------------------------------------------------

class TestCollectMastodon:
    def _account(self, id: str = "123") -> dict:
        return {"id": id, "followers_count": 500, "following_count": 100, "statuses_count": 200}

    def _post(self, id: str = "p1", created_at: str = RECENT, attachments: list | None = None) -> dict:
        return {
            "id": id, "created_at": created_at, "content": "<p>Hello</p>",
            "favourites_count": 5, "reblogs_count": 2, "replies_count": 1,
            "media_attachments": attachments or [], "url": f"https://hachyderm.io/@cate/{id}",
        }

    def test_happy_path_returns_posts(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            side_effect=[
                httpx.Response(200, json=[self._post("p1")]),
                httpx.Response(200, json=[]),  # second page → stops loop
            ]
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE)
        assert result is not None
        assert result["platform"] == "mastodon"
        assert len(result["posts"]) == 1
        assert result["posts"][0]["id"] == "p1"
        assert result["posts"][0]["favourites"] == 5

    def test_lookup_failure_returns_none(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(404)
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE)
        assert result is None

    def test_posts_before_since_excluded(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        # Old post stops pagination naturally (batch set to [])
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            return_value=httpx.Response(200, json=[self._post("p1"), self._post("p2", OLD)])
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE)
        assert len(result["posts"]) == 1
        assert result["posts"][0]["id"] == "p1"

    def test_account_follower_count(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            return_value=httpx.Response(200, json=[])
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE)
        assert result["account"]["followers"] == 500

    def test_no_access_token_no_follows(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            return_value=httpx.Response(200, json=[])
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE, access_token="")
        assert "new_follows" not in result

    def test_with_access_token_fetches_follows(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            return_value=httpx.Response(200, json=[])
        )
        respx_mock.get("https://hachyderm.io/api/v1/notifications").mock(
            return_value=httpx.Response(200, json=[
                {"created_at": RECENT, "account": {"acct": "friend@mastodon.social",
                  "display_name": "Friend", "followers_count": 100}},
            ], headers={})
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE, access_token="tok")
        assert "new_follows" in result
        assert result["new_follows"][0]["account"] == "friend@mastodon.social"

    def test_has_attachment_detected(self, respx_mock):
        respx_mock.get("https://hachyderm.io/api/v1/accounts/lookup").mock(
            return_value=httpx.Response(200, json=self._account())
        )
        respx_mock.get("https://hachyderm.io/api/v1/accounts/123/statuses").mock(
            side_effect=[
                httpx.Response(200, json=[self._post("p1", attachments=[{"type": "image"}])]),
                httpx.Response(200, json=[]),
            ]
        )
        result = collect_mastodon("hachyderm.io", "cate", since=SINCE)
        assert result["posts"][0]["has_attachment"] is True


# ---------------------------------------------------------------------------
# Bluesky
# ---------------------------------------------------------------------------

class TestCollectBluesky:
    def _feed_item(self, uri: str, created_at: str, text: str = "hello", reason: dict | None = None) -> dict:
        item: dict = {
            "post": {
                "uri": uri,
                "record": {"createdAt": created_at, "text": text},
                "likeCount": 3, "repostCount": 1, "replyCount": 0,
                "embed": {},
            }
        }
        if reason:
            item["reason"] = reason
        return item

    def test_happy_path_returns_posts(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(200, json={"did": "did:plc:abc"})
        )
        respx_mock.get("https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed").mock(
            return_value=httpx.Response(200, json={
                "feed": [self._feed_item("at://did:plc:abc/post/1", RECENT)],
                "cursor": None,
            })
        )
        result = collect_bluesky("catehstn.bsky.social", since=SINCE)
        assert result is not None
        assert result["platform"] == "bluesky"
        assert len(result["posts"]) == 1
        assert result["posts"][0]["likes"] == 3

    def test_failed_handle_resolve_returns_none(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(400, json={"error": "invalid handle"})
        )
        result = collect_bluesky("bad.handle", since=SINCE)
        assert result is None

    def test_posts_before_since_excluded(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(200, json={"did": "did:plc:abc"})
        )
        respx_mock.get("https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed").mock(
            return_value=httpx.Response(200, json={
                "feed": [
                    self._feed_item("at://did:plc:abc/post/1", RECENT),
                    self._feed_item("at://did:plc:abc/post/2", OLD),
                ],
                "cursor": None,
            })
        )
        result = collect_bluesky("catehstn.bsky.social", since=SINCE)
        assert len(result["posts"]) == 1

    def test_reposts_of_others_skipped(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(200, json={"did": "did:plc:abc"})
        )
        repost_item = self._feed_item(
            "at://did:plc:abc/post/1", RECENT,
            reason={"$type": "app.bsky.feed.defs#reasonRepost"},
        )
        respx_mock.get("https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed").mock(
            return_value=httpx.Response(200, json={"feed": [repost_item], "cursor": None})
        )
        result = collect_bluesky("catehstn.bsky.social", since=SINCE)
        assert len(result["posts"]) == 0

    def test_cursor_pagination_followed(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(200, json={"did": "did:plc:abc"})
        )
        respx_mock.get("https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed").mock(
            side_effect=[
                httpx.Response(200, json={
                    "feed": [self._feed_item("at://did:plc:abc/post/1", RECENT)],
                    "cursor": "cursor123",
                }),
                httpx.Response(200, json={
                    "feed": [self._feed_item("at://did:plc:abc/post/2", RECENT, "second page")],
                    "cursor": None,
                }),
            ]
        )
        result = collect_bluesky("catehstn.bsky.social", since=SINCE)
        assert len(result["posts"]) == 2

    def test_no_app_password_no_follows(self, respx_mock):
        respx_mock.get("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle").mock(
            return_value=httpx.Response(200, json={"did": "did:plc:abc"})
        )
        respx_mock.get("https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed").mock(
            return_value=httpx.Response(200, json={"feed": [], "cursor": None})
        )
        result = collect_bluesky("catehstn.bsky.social", since=SINCE, app_password="")
        assert "new_follows" not in result


# ---------------------------------------------------------------------------
# Buttondown
# ---------------------------------------------------------------------------

class TestCollectButtondown:
    def _newsletter(self, nl_id: str = "nl1", name: str = "my-newsletter", api_key: str = "key123") -> dict:
        return {"id": nl_id, "name": name, "api_key": api_key}

    def _email(self, email_id: str, subject: str, date: str) -> dict:
        return {
            "id": email_id, "subject": subject, "publish_date": date,
            "analytics": {"recipients": 500, "opens": 150, "clicks": 30,
                          "unsubscriptions": 2, "subscriptions": 5},
        }

    def test_happy_path_returns_emails(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(200, json={"results": [self._newsletter()]})
        )
        respx_mock.get("https://api.buttondown.email/v1/emails").mock(
            return_value=httpx.Response(200, json={
                "results": [self._email("e1", "Issue 1", RECENT)], "next": None,
            })
        )
        respx_mock.get("https://api.buttondown.email/v1/subscribers").mock(
            return_value=httpx.Response(200, json={"count": 520})
        )
        result = collect_buttondown("apikey", since=SINCE)
        assert result is not None
        assert result["platform"] == "buttondown"
        assert len(result["newsletters"]) == 1
        assert result["newsletters"][0]["id"] == "e1"

    def test_open_and_click_rates_calculated(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(200, json={"results": [self._newsletter()]})
        )
        respx_mock.get("https://api.buttondown.email/v1/emails").mock(
            return_value=httpx.Response(200, json={
                "results": [self._email("e1", "Issue 1", RECENT)], "next": None,
            })
        )
        respx_mock.get("https://api.buttondown.email/v1/subscribers").mock(
            return_value=httpx.Response(200, json={"count": 500})
        )
        result = collect_buttondown("apikey", since=SINCE)
        email = result["newsletters"][0]
        assert email["open_rate"] == round(150 / 500, 4)
        assert email["click_rate"] == round(30 / 500, 4)

    def test_subscriber_count_returned(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(200, json={"results": [self._newsletter()]})
        )
        respx_mock.get("https://api.buttondown.email/v1/emails").mock(
            return_value=httpx.Response(200, json={"results": [], "next": None})
        )
        respx_mock.get("https://api.buttondown.email/v1/subscribers").mock(
            return_value=httpx.Response(200, json={"count": 750})
        )
        result = collect_buttondown("apikey", since=SINCE)
        assert result["subscriber_counts"]["my-newsletter"] == 750

    def test_no_newsletters_returns_empty_not_none(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(200, json={"results": []})
        )
        result = collect_buttondown("apikey", since=SINCE)
        assert result is not None
        assert result["newsletters"] == []

    def test_api_error_returns_none(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(401, json={"error": "unauthorized"})
        )
        result = collect_buttondown("badkey", since=SINCE)
        assert result is None

    def test_emails_before_since_excluded(self, respx_mock):
        respx_mock.get("https://api.buttondown.email/v1/newsletters").mock(
            return_value=httpx.Response(200, json={"results": [self._newsletter()]})
        )
        respx_mock.get("https://api.buttondown.email/v1/emails").mock(
            return_value=httpx.Response(200, json={
                "results": [
                    self._email("e1", "Recent", RECENT),
                    self._email("e2", "Old", OLD),
                ],
                "next": None,
            })
        )
        respx_mock.get("https://api.buttondown.email/v1/subscribers").mock(
            return_value=httpx.Response(200, json={"count": 500})
        )
        result = collect_buttondown("apikey", since=SINCE)
        assert len(result["newsletters"]) == 1
        assert result["newsletters"][0]["id"] == "e1"


# ---------------------------------------------------------------------------
# Jetpack
# ---------------------------------------------------------------------------

class TestCollectJetpack:
    BASE = "https://public-api.wordpress.com/rest/v1.1/sites/cate.blog/stats"

    def _mock_all(self, respx_mock, top_posts_data: dict | None = None):
        respx_mock.get(f"{self.BASE}/visits").mock(
            return_value=httpx.Response(200, json={"data": [["2026-03-01", 100], ["2026-03-02", 120]]})
        )
        respx_mock.get(f"{self.BASE}/top-posts").mock(
            return_value=httpx.Response(200, json=top_posts_data or {"top-posts": [
                {"href": "https://cate.blog/post-a", "title": "Post A", "views": 80},
                {"href": "https://cate.blog/post-b", "title": "Post B", "views": 40},
            ]})
        )
        respx_mock.get(f"{self.BASE}/referrers").mock(
            return_value=httpx.Response(200, json={"days": {"2026-03-01": {"groups": [
                {"name": "twitter.com", "total": 30},
                {"name": "google.com", "total": 50},
            ]}}})
        )

    def test_happy_path(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_jetpack("cate.blog", "token", since=SINCE)
        assert result is not None
        assert result["platform"] == "jetpack"
        assert len(result["daily_views"]) == 2
        assert result["daily_views"][0] == {"date": "2026-03-01", "views": 100}

    def test_top_posts_returned(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_jetpack("cate.blog", "token", since=SINCE)
        assert len(result["top_posts"]) == 2
        assert result["top_posts"][0]["href"] == "https://cate.blog/post-a"

    def test_referrers_aggregated(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_jetpack("cate.blog", "token", since=SINCE)
        names = {r["name"] for r in result["referrers"]}
        assert "google.com" in names
        assert "twitter.com" in names

    def test_top_posts_days_format_aggregated(self, respx_mock):
        days_format = {"days": {
            "2026-03-01": {"postviews": [{"href": "https://cate.blog/a", "title": "A", "views": 50}]},
            "2026-03-02": {"postviews": [{"href": "https://cate.blog/a", "title": "A", "views": 30}]},
        }}
        self._mock_all(respx_mock, top_posts_data=days_format)
        result = collect_jetpack("cate.blog", "token", since=SINCE)
        assert len(result["top_posts"]) == 1
        assert result["top_posts"][0]["views"] == 80  # 50 + 30 aggregated

    def test_auth_failure_returns_none(self, respx_mock):
        respx_mock.get(f"{self.BASE}/visits").mock(return_value=httpx.Response(401))
        result = collect_jetpack("cate.blog", "badtoken", since=SINCE)
        assert result is None


# ---------------------------------------------------------------------------
# LinkedIn (file-based — no HTTP needed)
# ---------------------------------------------------------------------------

class TestCollectLinkedin:
    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _csv_row(self, date: str = "2026-03-01", impressions: int = 500) -> dict:
        return {"Post Title": "My Post", "Published Date": date, "Impressions": str(impressions),
                "Clicks": "20", "Reactions": "10", "Comments": "3", "Shares": "2"}

    def test_no_files_returns_none(self, tmp_path):
        result = collect_linkedin(linkedin_drops_dir=tmp_path)
        assert result is None

    def test_csv_parsed_correctly(self, tmp_path):
        self._write_csv(tmp_path / "export.csv", [self._csv_row()])
        result = collect_linkedin(linkedin_drops_dir=tmp_path)
        assert result is not None
        assert result["platform"] == "linkedin"
        assert result["posts"][0]["impressions"] == 500

    def test_most_recently_modified_wins(self, tmp_path):
        old_file = tmp_path / "old.csv"
        new_file = tmp_path / "new.csv"
        self._write_csv(old_file, [self._csv_row(impressions=100)])
        time.sleep(0.01)
        self._write_csv(new_file, [self._csv_row(impressions=999)])
        result = collect_linkedin(linkedin_drops_dir=tmp_path)
        assert result["posts"][0]["impressions"] == 999

    def test_result_includes_source_file_name(self, tmp_path):
        self._write_csv(tmp_path / "my_export.csv", [self._csv_row()])
        result = collect_linkedin(linkedin_drops_dir=tmp_path)
        assert result["source_file"] == "my_export.csv"

    def test_summary_totals_computed(self, tmp_path):
        self._write_csv(tmp_path / "export.csv", [
            self._csv_row(impressions=100),
            self._csv_row(date="2026-03-02", impressions=200),
        ])
        result = collect_linkedin(linkedin_drops_dir=tmp_path)
        assert result["summary"]["total_impressions"] == 300

    def test_recent_file_no_stale_warning(self, tmp_path, caplog):
        import logging
        self._write_csv(tmp_path / "export.csv", [self._csv_row()])
        with caplog.at_level(logging.WARNING, logger="collectors.linkedin"):
            collect_linkedin(linkedin_drops_dir=tmp_path)
        assert not any("days old" in r.message for r in caplog.records)

    def test_stale_file_logs_warning(self, tmp_path, caplog):
        import logging
        import os
        export = tmp_path / "export.csv"
        self._write_csv(export, [self._csv_row()])
        # Set mtime to 30 days ago
        stale_time = time.time() - (30 * 24 * 60 * 60)
        os.utime(export, (stale_time, stale_time))
        with caplog.at_level(logging.WARNING, logger="collectors.linkedin"):
            collect_linkedin(linkedin_drops_dir=tmp_path)
        assert any("days old" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Substack (file-based — no HTTP needed)
# ---------------------------------------------------------------------------

class TestCollectSubstack:
    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _csv_row(self, title: str = "My Post", date: str = "2026-03-01",
                 delivered: int = 500, open_rate: float = 0.4) -> dict:
        return {"title": title, "post_date": date, "delivered": str(delivered),
                "open_rate": str(open_rate), "likes": "10", "comments": "2", "shares": "1"}

    def test_no_files_returns_none(self, tmp_path):
        from collect import collect_substack
        result = collect_substack(substack_drops_dir=tmp_path)
        assert result is None

    def test_csv_parsed_correctly(self, tmp_path):
        from collect import collect_substack
        self._write_csv(tmp_path / "export.csv", [self._csv_row()])
        result = collect_substack(substack_drops_dir=tmp_path)
        assert result is not None
        assert result["platform"] == "substack"

    def test_recent_file_no_stale_warning(self, tmp_path, caplog):
        import logging
        from collect import collect_substack
        self._write_csv(tmp_path / "export.csv", [self._csv_row()])
        with caplog.at_level(logging.WARNING, logger="collectors.substack"):
            collect_substack(substack_drops_dir=tmp_path)
        assert not any("days old" in r.message for r in caplog.records)

    def test_stale_file_logs_warning(self, tmp_path, caplog):
        import logging
        import os
        from collect import collect_substack
        export = tmp_path / "export.csv"
        self._write_csv(export, [self._csv_row()])
        stale_time = time.time() - (30 * 24 * 60 * 60)
        os.utime(export, (stale_time, stale_time))
        with caplog.at_level(logging.WARNING, logger="collectors.substack"):
            collect_substack(substack_drops_dir=tmp_path)
        assert any("days old" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# O'Reilly (file-based — no HTTP needed)
# ---------------------------------------------------------------------------

# Minimal O'Reilly Payment Remittance Advice .eml — mirrors the real Oracle BI
# Publisher format. "Amount Withheld" column is intentionally empty (no text node)
# which is consistent with real files. Parser must handle 7-token data rows.
OREILLY_EML_TEMPLATE = """\
From: payables@oreilly.com
To: test@example.com
Subject: O'Reilly Payment Advice: paper document number - 999999
MIME-Version: 1.0
Content-Type: text/html; charset=UTF-8

<!DOCTYPE html>
<html><head><style>.x{{}}</style></head><body>
<table>
  <tr><td>Payment Remittance Advice</td></tr>
  <tr><td>{payment_date_display}</td></tr>
</table>
<table>
  <tr><td><b>Payment Reference Number</b></td><td>{payment_ref}</td></tr>
  <tr><td><b>Paper Document Number</b></td><td>{doc_number}</td></tr>
  <tr><td><b>Payment Date</b></td><td>{payment_date_display}</td></tr>
  <tr><td><b>Payment Currency</b></td><td>{currency}</td></tr>
  <tr><td><b>Payment Amount</b></td><td>{amount}</td></tr>
</table>
<table>
  <tr><td><b>Remittance Detail</b></td></tr>
  <tr>
    <td><b>Document Reference Number</b></td>
    <td><b>Document Date</b></td>
    <td><b>Description</b></td>
    <td><b>Document Amount</b></td>
    <td><b>Document Currency</b></td>
    <td><b>Amount Withheld</b></td>
    <td><b>Discount Taken</b></td>
    <td><b>Amount Paid</b></td>
  </tr>
  <tr>
    <td>{doc_ref}</td>
    <td>{doc_date}</td>
    <td>ROYALTY STATEMENT</td>
    <td>{amount}</td>
    <td>{currency}</td>
    <td></td>
    <td>.00</td>
    <td>{amount}</td>
  </tr>
  <tr><td></td><td></td><td></td><td></td><td><b>Total</b></td><td></td><td>.00</td><td>{amount}</td></tr>
</table>
</body></html>
"""


def _write_oreilly_eml(path, payment_date="Jan 15, 2026", amount="250.00",
                       currency="USD", doc_number="210000",
                       payment_ref="123456", doc_ref="AP-1234567",
                       doc_date="14-Jan-26"):
    content = OREILLY_EML_TEMPLATE.format(
        payment_date_display=payment_date, amount=amount, currency=currency,
        doc_number=doc_number, payment_ref=payment_ref,
        doc_ref=doc_ref, doc_date=doc_date,
    )
    path.write_text(content)


class TestCollectOreilly:
    def test_no_files_returns_none(self, tmp_path):
        from collect import collect_oreilly
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert result is None

    def test_single_payment_parsed(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "payment.eml")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert result is not None
        assert result["platform"] == "oreilly"
        assert result["payment_count"] == 1
        assert result["payments"][0]["amount"] == 250.0
        assert result["payments"][0]["currency"] == "USD"
        assert result["payments"][0]["payment_date"] == "2026-01-15"

    def test_total_paid_summed_across_files(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "a.eml", amount="100.00", doc_number="111111", payment_ref="111", doc_ref="AP-111")
        _write_oreilly_eml(tmp_path / "b.eml", amount="200.00", doc_number="222222", payment_ref="222", doc_ref="AP-222")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert result["total_paid"] == pytest.approx(300.0)
        assert result["payment_count"] == 2

    def test_payments_sorted_by_date(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "b.eml", payment_date="Mar 01, 2026", doc_number="222222", payment_ref="222", doc_ref="AP-222")
        _write_oreilly_eml(tmp_path / "a.eml", payment_date="Jan 15, 2026", doc_number="111111", payment_ref="111", doc_ref="AP-111")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        dates = [p["payment_date"] for p in result["payments"]]
        assert dates == sorted(dates)

    def test_line_items_extracted(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "payment.eml", amount="359.79", doc_ref="AP-1410830")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        items = result["payments"][0]["line_items"]
        assert len(items) == 1
        assert items[0]["doc_ref"] == "AP-1410830"
        assert items[0]["description"] == "ROYALTY STATEMENT"
        assert items[0]["amount_paid"] == pytest.approx(359.79)

    def test_source_file_name_recorded(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "my_payment.eml")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert result["payments"][0]["source_file"] == "my_payment.eml"

    def test_currencies_collected(self, tmp_path):
        from collect import collect_oreilly
        _write_oreilly_eml(tmp_path / "payment.eml", currency="USD")
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert "USD" in result["currencies"]

    def test_withheld_amount_parsed_when_present(self, tmp_path):
        """US authors may have tax withheld — 8-token row with Amount Withheld populated."""
        from collect import collect_oreilly
        # Build an eml with a non-empty Amount Withheld cell
        content = OREILLY_EML_TEMPLATE.replace(
            "<td></td>\n    <td>.00</td>",
            "<td>25.00</td>\n    <td>.00</td>",
        )
        (tmp_path / "payment.eml").write_text(content.format(
            payment_date_display="Jan 15, 2026", amount="250.00", currency="USD",
            doc_number="210000", payment_ref="123456",
            doc_ref="AP-1234567", doc_date="14-Jan-26",
        ))
        result = collect_oreilly(oreilly_drops_dir=tmp_path)
        assert result is not None
        items = result["payments"][0]["line_items"]
        assert len(items) == 1
        assert items[0]["amount_withheld"] == pytest.approx(25.0)


# ---------------------------------------------------------------------------
# Amazon
# ---------------------------------------------------------------------------

AMAZON_HTML = """
<html><body>
<span id="productTitle">My Book Title</span>
<span>4.5 out of 5 stars</span>
<span aria-label="123 Reviews" class="acrCustomerReviewText">123 Reviews</span>
<span>#1,234 in Books</span>
</body></html>
"""


class TestCollectAmazon:
    def test_happy_path_returns_data(self, respx_mock):
        respx_mock.get("https://www.amazon.com/dp/B0CW1MYCGK").mock(
            return_value=httpx.Response(200, text=AMAZON_HTML)
        )
        result = collect_amazon(["B0CW1MYCGK"], marketplaces=["amazon.com"])
        assert result is not None
        assert result["platform"] == "amazon"
        book = result["by_marketplace"]["amazon.com"][0]
        assert book["asin"] == "B0CW1MYCGK"
        assert book["rating"] == 4.5
        assert book["best_sellers_rank"] == 1234

    def test_no_asins_returns_none(self):
        result = collect_amazon([], marketplaces=["amazon.com"])
        assert result is None

    def test_404_for_one_asin_skipped(self, respx_mock, monkeypatch):
        monkeypatch.setattr("collectors.amazon.time.sleep", lambda _: None)
        respx_mock.get("https://www.amazon.com/dp/BADASIN").mock(
            return_value=httpx.Response(404)
        )
        respx_mock.get("https://www.amazon.com/dp/B0CW1MYCGK").mock(
            return_value=httpx.Response(200, text=AMAZON_HTML)
        )
        result = collect_amazon(["BADASIN", "B0CW1MYCGK"], marketplaces=["amazon.com"])
        assert result is not None
        asins = [b["asin"] for b in result["by_marketplace"]["amazon.com"]]
        assert "BADASIN" not in asins
        assert "B0CW1MYCGK" in asins

    def test_all_asins_fail_returns_none(self, respx_mock):
        respx_mock.get("https://www.amazon.com/dp/BAD1").mock(return_value=httpx.Response(404))
        result = collect_amazon(["BAD1"], marketplaces=["amazon.com"])
        assert result is None

    def test_multiple_marketplaces_all_queried(self, respx_mock):
        respx_mock.get("https://www.amazon.com/dp/B0CW1MYCGK").mock(
            return_value=httpx.Response(200, text=AMAZON_HTML)
        )
        respx_mock.get("https://www.amazon.co.uk/dp/B0CW1MYCGK").mock(
            return_value=httpx.Response(200, text=AMAZON_HTML)
        )
        result = collect_amazon(["B0CW1MYCGK"], marketplaces=["amazon.com", "amazon.co.uk"])
        assert "amazon.com" in result["by_marketplace"]
        assert "amazon.co.uk" in result["by_marketplace"]


# ---------------------------------------------------------------------------
# Vercel
# ---------------------------------------------------------------------------

class TestCollectVercel:
    BASE = "https://vercel.com/api/web-analytics"

    def _mock_all(self, respx_mock):
        respx_mock.get(f"{self.BASE}/overview").mock(
            return_value=httpx.Response(200, json={"total": 1000, "devices": 800, "bounceRate": 45.0})
        )
        respx_mock.get(f"{self.BASE}/timeseries").mock(
            return_value=httpx.Response(200, json={"data": {"groups": {"all": [
                {"key": "2026-03-01", "total": 500, "devices": 400},
                {"key": "2026-03-02", "total": 500, "devices": 400},
            ]}}}),
        )
        respx_mock.get(f"{self.BASE}/stats").mock(
            return_value=httpx.Response(200, json={"data": [{"key": "/", "total": 300, "devices": 250}]})
        )

    def test_happy_path(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_vercel("tok", "my-project", since=SINCE)
        assert result is not None
        assert result["platform"] == "vercel"
        assert result["page_views"] == 1000
        assert result["visitors"] == 800
        assert len(result["daily"]) == 2

    def test_daily_entries_mapped(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_vercel("tok", "my-project", since=SINCE)
        assert result["daily"][0] == {"date": "2026-03-01", "page_views": 500, "visitors": 400}

    def test_with_team_id_does_not_crash(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_vercel("tok", "my-project", team_id="team_abc", since=SINCE)
        assert result is not None

    def test_api_error_returns_none(self, respx_mock):
        respx_mock.get(f"{self.BASE}/overview").mock(return_value=httpx.Response(401))
        result = collect_vercel("badtok", "my-project", since=SINCE)
        assert result is None


# ---------------------------------------------------------------------------
# GoatCounter
# ---------------------------------------------------------------------------

class TestCollectGoatcounter:
    BASE = "https://what-raccoon.goatcounter.com/api/v0"

    def _mock_all(self, respx_mock, hits=None, total=None):
        respx_mock.get(f"{self.BASE}/stats/total").mock(
            return_value=httpx.Response(200, json=total or {"total": 1500, "total_unique": 900})
        )
        respx_mock.get(f"{self.BASE}/stats/hits").mock(
            return_value=httpx.Response(200, json={"hits": hits or [
                {"path": "/", "count": 1300},
                {"path": "/about", "count": 200},
                {"path": "result/trike", "count": 120},
                {"path": "result/mpr", "count": 80},
            ]})
        )

    def test_happy_path_returns_data(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        assert result is not None
        assert result["platform"] == "goatcounter"
        assert result["total_pageviews"] == 1500
        assert result["total_unique"] == 900

    def test_page_paths_in_top_paths(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        paths = [h["path"] for h in result["top_paths"]]
        assert "/" in paths
        assert "/about" in paths

    def test_events_separated_from_paths(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        events = [e["event"] for e in result["events"]]
        assert "result/trike" in events
        assert "result/mpr" in events

    def test_events_not_in_top_paths(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        paths = [h["path"] for h in result["top_paths"]]
        assert not any(p.startswith("result/") for p in paths)

    def test_no_events_returns_empty_list(self, respx_mock):
        self._mock_all(respx_mock, hits=[{"path": "/", "count": 500}])
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        assert result["events"] == []

    def test_api_error_returns_none(self, respx_mock):
        respx_mock.get(f"{self.BASE}/stats/total").mock(
            return_value=httpx.Response(401, json={"error": "unauthorized"})
        )
        result = collect_goatcounter("what-raccoon", "badtoken", since=SINCE)
        assert result is None

    def test_period_dates_in_result(self, respx_mock):
        self._mock_all(respx_mock)
        result = collect_goatcounter("what-raccoon", "token", since=SINCE)
        assert result["period_start"] == SINCE.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# collect_mentions
# ---------------------------------------------------------------------------

def _hn_hit(domain: str, tag: str) -> dict:
    return {
        "objectID": f"{domain}-{tag}-1",
        "title": f"Found {domain}", "url": f"https://{domain}/post",
        "points": 10, "num_comments": 2,
        "created_at": RECENT[:10], "author": "someone",
    }


class TestCollectMentions:
    def test_empty_domains_returns_none(self):
        result = collect_mentions(domains=[], since=SINCE)
        assert result is None

    def test_single_domain_queries_hn_story_and_comment(self, respx_mock):
        hn = respx_mock.get("https://hn.algolia.com/api/v1/search_by_date")
        hn.mock(side_effect=lambda req: httpx.Response(200, json={"hits": [
            _hn_hit(req.url.params["query"], req.url.params["tags"])
        ]}))
        result = collect_mentions(domains=["cate.blog"], since=SINCE)
        assert hn.call_count == 2  # story + comment
        assert result is not None

    def test_multiple_domains_all_queried(self, respx_mock):
        hn = respx_mock.get("https://hn.algolia.com/api/v1/search_by_date")
        hn.mock(side_effect=lambda req: httpx.Response(200, json={"hits": [
            _hn_hit(req.url.params["query"], req.url.params["tags"])
        ]}))
        domains = ["cate.blog", "driyourcareer.com", "whatsmyjob.club"]
        collect_mentions(domains=domains, since=SINCE)
        assert hn.call_count == len(domains) * 2

    def test_each_domain_used_as_hn_query(self, respx_mock):
        hn = respx_mock.get("https://hn.algolia.com/api/v1/search_by_date")
        hn.mock(side_effect=lambda req: httpx.Response(200, json={"hits": [
            _hn_hit(req.url.params["query"], req.url.params["tags"])
        ]}))
        domains = ["cate.blog", "driyourcareer.com"]
        collect_mentions(domains=domains, since=SINCE)
        queried = {req.url.params["query"] for req, _ in hn.calls}
        assert queried == set(domains)

    def test_hn_hit_not_matching_domain_filtered(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": [
                {"objectID": "x1", "title": "Unrelated content", "url": "https://other.com/",
                 "points": 5, "num_comments": 0, "created_at": RECENT[:10], "author": "user"},
            ]})
        )
        result = collect_mentions(domains=["cate.blog"], since=SINCE)
        assert result["sources"]["hacker_news"] == []

    def test_no_mastodon_token_skips_mastodon(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        result = collect_mentions(
            domains=["cate.blog"], since=SINCE,
            mastodon_instance="hachyderm.io", mastodon_access_token="",
        )
        assert "mastodon" not in result["sources"]

    def test_mastodon_mentions_fetched_with_token(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        respx_mock.get("https://hachyderm.io/api/v1/notifications").mock(
            return_value=httpx.Response(200, json=[
                {"created_at": RECENT, "account": {"acct": "friend@m.social"},
                 "status": {"content": "<p>great post</p>", "url": ""}},
            ], headers={})
        )
        result = collect_mentions(
            domains=["cate.blog"], since=SINCE,
            mastodon_instance="hachyderm.io", mastodon_access_token="tok",
        )
        assert "mastodon" in result["sources"]
        assert result["sources"]["mastodon"][0]["from"] == "friend@m.social"

    def test_mastodon_pagination_capped(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        next_link = '<https://hachyderm.io/api/v1/notifications?max_id=99>; rel="next"'
        masto = respx_mock.get("https://hachyderm.io/api/v1/notifications")
        masto.mock(return_value=httpx.Response(200, json=[
            {"created_at": RECENT, "account": {"acct": "x@m.social"},
             "status": {"content": "hi", "url": ""}}
        ], headers={"Link": next_link}))
        collect_mentions(
            domains=["cate.blog"], since=SINCE,
            mastodon_instance="hachyderm.io", mastodon_access_token="tok",
        )
        assert masto.call_count <= 5

    def test_no_bluesky_password_skips_bluesky(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        result = collect_mentions(
            domains=["cate.blog"], since=SINCE,
            bluesky_handle="catehstn.bsky.social", bluesky_app_password="",
        )
        assert "bluesky" not in result["sources"]

    def test_bluesky_mentions_fetched_with_password(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        respx_mock.post("https://bsky.social/xrpc/com.atproto.server.createSession").mock(
            return_value=httpx.Response(200, json={"accessJwt": "jwt123"})
        )
        respx_mock.get("https://bsky.social/xrpc/app.bsky.notification.listNotifications").mock(
            return_value=httpx.Response(200, json={"notifications": [
                {"reason": "mention", "indexedAt": RECENT,
                 "author": {"handle": "friend.bsky.social"},
                 "record": {"text": "nice post"},
                 "uri": "at://did:plc:x/app.bsky.feed.post/abc"},
            ], "cursor": None})
        )
        result = collect_mentions(
            domains=["cate.blog"], since=SINCE,
            bluesky_handle="catehstn.bsky.social", bluesky_app_password="pass",
        )
        assert "bluesky" in result["sources"]
        assert result["sources"]["bluesky"][0]["from"] == "friend.bsky.social"

    def test_bluesky_non_mention_notifications_excluded(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        respx_mock.post("https://bsky.social/xrpc/com.atproto.server.createSession").mock(
            return_value=httpx.Response(200, json={"accessJwt": "jwt123"})
        )
        respx_mock.get("https://bsky.social/xrpc/app.bsky.notification.listNotifications").mock(
            return_value=httpx.Response(200, json={"notifications": [
                {"reason": "like", "indexedAt": RECENT, "author": {"handle": "liker.bsky.social"},
                 "record": {"text": ""}, "uri": "at://x"},
                {"reason": "follow", "indexedAt": RECENT, "author": {"handle": "follower.bsky.social"},
                 "record": {"text": ""}, "uri": "at://y"},
            ], "cursor": None})
        )
        result = collect_mentions(
            domains=["cate.blog"], since=SINCE,
            bluesky_handle="catehstn.bsky.social", bluesky_app_password="pass",
        )
        assert result["sources"]["bluesky"] == []

    def test_no_gsc_credentials_skips_gsc(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        result = collect_mentions(domains=["cate.blog"], since=SINCE, gsc_credentials_file="")
        assert "google_search_console" not in result["sources"]

    def test_gsc_domain_property_success(self, tmp_path, respx_mock):
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        mock_service = MagicMock()
        mock_service.searchanalytics().query().execute.return_value = {
            "rows": [{"keys": ["engineering mgmt", "https://cate.blog/p"],
                      "clicks": 5, "impressions": 100, "ctr": 0.05, "position": 8.0}]
        }
        with patch("google.oauth2.service_account.Credentials") as mock_creds, \
             patch("googleapiclient.discovery.build", return_value=mock_service):
            mock_creds.from_service_account_file.return_value = MagicMock()
            result = collect_mentions(
                domains=["cate.blog"], since=SINCE,
                gsc_credentials_file=str(creds_file),
            )
        assert "google_search_console" in result["sources"]
        assert result["sources"]["google_search_console"][0]["query"] == "engineering mgmt"

    def test_gsc_all_properties_fail_warns_no_crash(self, tmp_path, respx_mock, caplog):
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        mock_service = MagicMock()
        mock_service.searchanalytics().query().execute.side_effect = Exception("403 Forbidden")
        import logging
        with patch("google.oauth2.service_account.Credentials") as mock_creds, \
             patch("googleapiclient.discovery.build", return_value=mock_service), \
             caplog.at_level(logging.WARNING, logger="collectors.mentions"):
            mock_creds.from_service_account_file.return_value = MagicMock()
            result = collect_mentions(
                domains=["cate.blog"], since=SINCE,
                gsc_credentials_file=str(creds_file),
            )
        assert result is not None
        assert result["sources"]["google_search_console"] == []
        assert any("no accessible property" in r.message for r in caplog.records)

    def test_gsc_queries_all_domains(self, tmp_path, respx_mock):
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            return_value=httpx.Response(200, json={"hits": []})
        )
        queried_sites: list[str] = []

        def fake_query(siteUrl, body):
            queried_sites.append(siteUrl)
            m = MagicMock()
            m.execute.return_value = {"rows": []}
            return m

        mock_service = MagicMock()
        mock_service.searchanalytics().query.side_effect = fake_query

        domains = ["cate.blog", "driyourcareer.com", "whatsmyjob.club"]
        with patch("google.oauth2.service_account.Credentials") as mock_creds, \
             patch("googleapiclient.discovery.build", return_value=mock_service):
            mock_creds.from_service_account_file.return_value = MagicMock()
            collect_mentions(domains=domains, since=SINCE, gsc_credentials_file=str(creds_file))

        # sc-domain:<domain> is tried first for each domain
        queried_domain_names = {s.replace("sc-domain:", "").replace("https://", "").replace("http://", "").rstrip("/")
                                 for s in queried_sites}
        for d in domains:
            assert d in queried_domain_names

    def test_all_sources_fail_returns_none(self, respx_mock):
        respx_mock.get("https://hn.algolia.com/api/v1/search_by_date").mock(
            side_effect=httpx.ConnectError("connection failed")
        )
        result = collect_mentions(domains=["cate.blog"], since=SINCE)
        assert result is None


# ---------------------------------------------------------------------------
# collect_all
# ---------------------------------------------------------------------------

class TestCollectAll:
    def test_missing_mastodon_instance_skips(self):
        result = collect_all({"mastodon_handle": "cate"}, platform="mastodon", since=SINCE)
        assert "mastodon" not in result

    def test_missing_bluesky_handle_skips(self):
        result = collect_all({}, platform="bluesky", since=SINCE)
        assert "bluesky" not in result

    def test_missing_buttondown_key_skips(self):
        result = collect_all({}, platform="buttondown", since=SINCE)
        assert "buttondown" not in result

    def test_missing_jetpack_token_skips(self):
        result = collect_all({"jetpack_site": "cate.blog"}, platform="jetpack", since=SINCE)
        assert "jetpack" not in result

    def test_missing_amazon_asins_skips(self):
        result = collect_all({}, platform="amazon", since=SINCE)
        assert "amazon" not in result

    def test_missing_vercel_token_skips(self):
        result = collect_all({"vercel_project_id": "proj"}, platform="vercel", since=SINCE)
        assert "vercel" not in result

    def test_missing_monitored_domains_skips_mentions(self):
        result = collect_all({}, platform="mentions", since=SINCE)
        assert "mentions" not in result

    def test_missing_goatcounter_site_skips(self):
        result = collect_all({"goatcounter_token": "tok"}, platform="goatcounter", since=SINCE)
        assert "goatcounter" not in result

    def test_missing_goatcounter_token_skips(self):
        result = collect_all({"goatcounter_site": "what-raccoon"}, platform="goatcounter", since=SINCE)
        assert "goatcounter" not in result

    def test_missing_calendly_token_skips(self):
        result = collect_all({}, platform="calendly", since=SINCE)
        assert "calendly" not in result

    def test_collector_returning_none_absent_from_results(self, monkeypatch):
        monkeypatch.setattr("collectors._dispatch.collect_mastodon", lambda *a, **kw: None)
        config = {"mastodon_instance": "hachyderm.io", "mastodon_handle": "cate"}
        result = collect_all(config, platform="mastodon", since=SINCE)
        assert "mastodon" not in result

    def test_platform_filter_runs_only_one_collector(self, monkeypatch):
        called = []
        monkeypatch.setattr("collectors._dispatch.collect_mastodon", lambda *a, **kw: called.append("mastodon") or {})
        monkeypatch.setattr("collectors._dispatch.collect_bluesky", lambda *a, **kw: called.append("bluesky") or {})
        config = {
            "mastodon_instance": "hachyderm.io", "mastodon_handle": "cate",
            "bluesky_handle": "x.bsky.social",
        }
        collect_all(config, platform="mastodon", since=SINCE)
        assert called == ["mastodon"]


# ---------------------------------------------------------------------------
# collect_calendly
# ---------------------------------------------------------------------------

USER_URI = "https://api.calendly.com/users/abc123"
ET_URI_INTRO = "https://api.calendly.com/event_types/intro"
ET_URI_CONSULT = "https://api.calendly.com/event_types/consult"


def _calendly_mock(respx_mock):
    """Wire up standard Calendly API mock responses."""
    respx_mock.get("https://api.calendly.com/users/me").mock(
        return_value=httpx.Response(200, json={"resource": {"uri": USER_URI}})
    )
    respx_mock.get("https://api.calendly.com/event_types").mock(
        return_value=httpx.Response(200, json={
            "collection": [
                {"uri": ET_URI_INTRO, "name": "Intro call"},
                {"uri": ET_URI_CONSULT, "name": "Consultation"},
            ]
        })
    )


@pytest.mark.respx(base_url="https://api.calendly.com")
class TestCollectCalendly:
    def test_returns_expected_keys(self, respx_mock):
        _calendly_mock(respx_mock)
        respx_mock.get("https://api.calendly.com/scheduled_events").mock(
            return_value=httpx.Response(200, json={"collection": []})
        )
        result = collect_calendly("tok", since=SINCE)
        assert result is not None
        assert result["platform"] == "calendly"
        assert "total_bookings" in result
        assert "total_canceled" in result
        assert "bookings_by_type" in result

    def test_aggregates_active_bookings_by_type(self, respx_mock):
        _calendly_mock(respx_mock)
        active = [
            {"event_type": ET_URI_INTRO},
            {"event_type": ET_URI_INTRO},
            {"event_type": ET_URI_CONSULT},
        ]

        def side_effect(request):
            params = dict(request.url.params)
            if params.get("status") == "active":
                return httpx.Response(200, json={"collection": active})
            return httpx.Response(200, json={"collection": []})

        respx_mock.get("https://api.calendly.com/scheduled_events").mock(side_effect=side_effect)
        result = collect_calendly("tok", since=SINCE)
        assert result["total_bookings"] == 3
        by_name = {e["event_type"]: e for e in result["bookings_by_type"]}
        assert by_name["Intro call"]["active"] == 2
        assert by_name["Consultation"]["active"] == 1

    def test_aggregates_canceled_bookings(self, respx_mock):
        _calendly_mock(respx_mock)
        canceled = [{"event_type": ET_URI_INTRO}]

        def side_effect(request):
            params = dict(request.url.params)
            if params.get("status") == "canceled":
                return httpx.Response(200, json={"collection": canceled})
            return httpx.Response(200, json={"collection": []})

        respx_mock.get("https://api.calendly.com/scheduled_events").mock(side_effect=side_effect)
        result = collect_calendly("tok", since=SINCE)
        assert result["total_canceled"] == 1
        by_name = {e["event_type"]: e for e in result["bookings_by_type"]}
        assert by_name["Intro call"]["canceled"] == 1

    def test_unknown_event_type_uri_uses_id_fragment(self, respx_mock):
        respx_mock.get("https://api.calendly.com/users/me").mock(
            return_value=httpx.Response(200, json={"resource": {"uri": USER_URI}})
        )
        respx_mock.get("https://api.calendly.com/event_types").mock(
            return_value=httpx.Response(200, json={"collection": []})
        )
        unknown_uri = "https://api.calendly.com/event_types/mystery-type"

        def side_effect(request):
            params = dict(request.url.params)
            if params.get("status") == "active":
                return httpx.Response(200, json={"collection": [{"event_type": unknown_uri}]})
            return httpx.Response(200, json={"collection": []})

        respx_mock.get("https://api.calendly.com/scheduled_events").mock(side_effect=side_effect)
        result = collect_calendly("tok", since=SINCE)
        assert result["total_bookings"] == 1
        assert result["bookings_by_type"][0]["event_type"] == "mystery-type"

    def test_empty_calendar_returns_zeros(self, respx_mock):
        _calendly_mock(respx_mock)
        respx_mock.get("https://api.calendly.com/scheduled_events").mock(
            return_value=httpx.Response(200, json={"collection": []})
        )
        result = collect_calendly("tok", since=SINCE)
        assert result["total_bookings"] == 0
        assert result["total_canceled"] == 0
        assert result["bookings_by_type"] == []

    def test_network_error_returns_none(self, respx_mock):
        respx_mock.get("https://api.calendly.com/users/me").mock(
            side_effect=httpx.ConnectError("connection failed")
        )
        result = collect_calendly("tok", since=SINCE)
        assert result is None

    def test_auth_error_returns_none(self, respx_mock):
        respx_mock.get("https://api.calendly.com/users/me").mock(
            return_value=httpx.Response(401, json={"message": "Unauthorized"})
        )
        result = collect_calendly("tok", since=SINCE)
        assert result is None

    def test_lead_gen_event_surfaced_separately(self, respx_mock):
        _calendly_mock(respx_mock)
        active = [
            {"event_type": ET_URI_INTRO},
            {"event_type": ET_URI_INTRO},
            {"event_type": ET_URI_CONSULT},
        ]

        def side_effect(request):
            params = dict(request.url.params)
            if params.get("status") == "active":
                return httpx.Response(200, json={"collection": active})
            return httpx.Response(200, json={"collection": []})

        respx_mock.get("https://api.calendly.com/scheduled_events").mock(side_effect=side_effect)
        result = collect_calendly("tok", since=SINCE, lead_gen_event="Intro call")
        assert result["lead_gen_event"] == "Intro call"
        assert result["lead_gen_bookings"] == 2

    def test_lead_gen_event_missing_returns_zero(self, respx_mock):
        _calendly_mock(respx_mock)
        respx_mock.get("https://api.calendly.com/scheduled_events").mock(
            return_value=httpx.Response(200, json={"collection": []})
        )
        result = collect_calendly("tok", since=SINCE, lead_gen_event="Nonexistent Event")
        assert result["lead_gen_bookings"] == 0

    def test_no_lead_gen_event_omits_field(self, respx_mock):
        _calendly_mock(respx_mock)
        respx_mock.get("https://api.calendly.com/scheduled_events").mock(
            return_value=httpx.Response(200, json={"collection": []})
        )
        result = collect_calendly("tok", since=SINCE)
        assert "lead_gen_bookings" not in result
        assert "lead_gen_event" not in result
