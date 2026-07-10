"""
Tests for collectors/stripe.py and its wiring.
All HTTP is mocked with respx; no real Stripe key needed.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import pytest
import respx

from collectors.stripe import collect_stripe
from collectors._dispatch import _resolve_stripe_tokens, _stripe_since
from store import _process_stripe, _load

SINCE = datetime(2025, 12, 1, tzinfo=timezone.utc)
STRIPE = "https://api.stripe.com/v1"


def _products_response(items):
    return {"object": "list", "has_more": False, "data": items}


def _paginated(items, has_more=False):
    return {"object": "list", "has_more": has_more, "data": items}


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------

class TestResolveTokens:
    def test_dict_form(self):
        cfg = {"stripe_tokens": {"a": "rk_1", "b": "rk_2"}}
        assert _resolve_stripe_tokens(cfg) == {"a": "rk_1", "b": "rk_2"}

    def test_flat_form(self):
        cfg = {"stripe_token_primary": "rk_1", "stripe_token_secondary": "rk_2"}
        assert _resolve_stripe_tokens(cfg) == {"primary": "rk_1", "secondary": "rk_2"}

    def test_mixed(self):
        cfg = {"stripe_tokens": {"a": "rk_1"}, "stripe_token_b": "rk_2"}
        assert _resolve_stripe_tokens(cfg) == {"a": "rk_1", "b": "rk_2"}

    def test_flat_overrides_dict_on_same_label(self):
        cfg = {"stripe_tokens": {"a": "rk_1"}, "stripe_token_a": "rk_2"}
        # Flat form runs second, wins
        assert _resolve_stripe_tokens(cfg)["a"] == "rk_2"

    def test_ignores_empty_values(self):
        cfg = {"stripe_tokens": {"a": "", "b": None}, "stripe_token_c": ""}
        assert _resolve_stripe_tokens(cfg) == {}

    def test_ignores_unrelated_keys(self):
        cfg = {"stripe_token": "should-not-appear", "other_key": "x"}
        # "stripe_token" (no trailing _label) has no suffix → skipped
        assert _resolve_stripe_tokens(cfg) == {}


# ---------------------------------------------------------------------------
# Lookback window (_stripe_since)
# ---------------------------------------------------------------------------

class TestStripeSince:
    def test_explicit_since_wins(self):
        # An explicit global since always overrides stripe_since_days.
        assert _stripe_since({"stripe_since_days": 5}, SINCE) == SINCE

    def test_default_is_60_days(self):
        before = datetime.now(timezone.utc) - timedelta(days=60)
        got = _stripe_since({}, None)
        after = datetime.now(timezone.utc) - timedelta(days=60)
        assert before <= got <= after

    def test_configured_days_used(self):
        before = datetime.now(timezone.utc) - timedelta(days=90)
        got = _stripe_since({"stripe_since_days": 90}, None)
        after = datetime.now(timezone.utc) - timedelta(days=90)
        assert before <= got <= after

    def test_non_positive_falls_back_to_collector_default(self):
        # 0 / negative → None, letting collect_stripe use its own default.
        assert _stripe_since({"stripe_since_days": 0}, None) is None
        assert _stripe_since({"stripe_since_days": -3}, None) is None

    def test_bad_value_falls_back_to_60(self):
        before = datetime.now(timezone.utc) - timedelta(days=60)
        got = _stripe_since({"stripe_since_days": "not-a-number"}, None)
        after = datetime.now(timezone.utc) - timedelta(days=60)
        assert before <= got <= after

    def test_no_tokens_returns_empty(self):
        assert _resolve_stripe_tokens({}) == {}


# ---------------------------------------------------------------------------
# collect_stripe
# ---------------------------------------------------------------------------

class TestCollectStripe:
    def test_empty_tokens_returns_none(self):
        assert collect_stripe({}, since=SINCE) is None

    def test_all_empty_tokens_returns_none(self):
        assert collect_stripe({"a": "", "b": None}, since=SINCE) is None

    def test_happy_path_single_account(self, respx_mock):
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([
                {"id": "prod_1", "name": "Course A", "active": True},
            ]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "ch_1", "status": "succeeded", "amount": 49900,
                 "amount_refunded": 0, "currency": "usd",
                 "created": int(datetime(2026, 2, 15, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_2", "status": "succeeded", "amount": 24950,
                 "amount_refunded": 0, "currency": "usd",
                 "created": int(datetime(2026, 3, 3, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_3", "status": "failed", "amount": 10000,
                 "amount_refunded": 0, "currency": "usd",
                 "created": int(datetime(2026, 3, 5, tzinfo=timezone.utc).timestamp())},
            ]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "cs_1", "payment_status": "paid", "amount_total": 49900,
                 "currency": "usd", "created": 1740000000,
                 "metadata": {"courseName": "Course A", "cohortName": "Feb 2026"},
                 "customer_details": {"email": "a@b.com"}},
                {"id": "cs_2", "payment_status": "unpaid", "amount_total": 100,
                 "currency": "usd", "created": 1740000100,
                 "metadata": {}},
            ]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "in_1", "status": "paid", "amount_paid": 449500,
                 "currency": "usd", "created": 1741000000,
                 "lines": {"data": [
                     {"description": "Course A Enrollment", "amount": 449500},
                 ]}},
            ]))
        )

        result = collect_stripe({"primary": "rk_test"}, since=SINCE)

        assert result is not None
        assert result["platform"] == "stripe"
        assert set(result["accounts"].keys()) == {"primary"}
        acct = result["accounts"]["primary"]

        assert acct["charges_succeeded"] == 2  # failed charge excluded
        assert acct["gross_cents"] == 49900 + 24950
        assert len(acct["monthly"]) == 2
        # sessions: only paid ones flow through
        assert len(acct["paid_sessions"]) == 1
        assert acct["paid_sessions"][0]["metadata"]["courseName"] == "Course A"
        # invoice
        assert len(acct["paid_invoices"]) == 1
        assert acct["paid_invoices"][0]["lines"][0]["description"] == "Course A Enrollment"
        # totals rollup
        assert result["totals"]["gross_cents"] == 74850

    def test_invoices_permission_denied_soft_fails(self, respx_mock):
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(403, json={"error": {"message": "no perm"}})
        )
        result = collect_stripe({"primary": "rk_test"}, since=SINCE)
        assert result is not None
        assert result["accounts"]["primary"]["paid_invoices"] == []

    def test_products_hard_fail_skips_account(self, respx_mock):
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(401, json={"error": {"message": "bad key"}})
        )
        result = collect_stripe({"primary": "rk_bad"}, since=SINCE)
        assert result is None

    def test_multi_account_partial_failure(self, respx_mock):
        # Route both keys through matching responses. respx matches on URL only,
        # not auth — so we simulate one account working and the other failing
        # by making products fail for the SECOND call using side_effect.
        respx_mock.get(f"{STRIPE}/products").mock(side_effect=[
            httpx.Response(200, json=_products_response([])),
            httpx.Response(401, json={"error": {"message": "bad"}}),
        ])
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )

        result = collect_stripe({"good": "rk_1", "bad": "rk_2"}, since=SINCE)
        assert result is not None
        assert set(result["accounts"].keys()) == {"good"}

    def test_pagination_partial_failure_returns_partial(self, respx_mock, caplog):
        """A mid-pagination failure should return the pages already fetched."""
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        # Two charges pages: first succeeds with has_more=True, second 503s.
        page1 = _paginated([
            {"id": "ch_1", "status": "succeeded", "amount": 1000, "currency": "usd",
             "created": int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())},
        ], has_more=True)
        respx_mock.get(f"{STRIPE}/charges").mock(side_effect=[
            httpx.Response(200, json=page1),
            httpx.Response(503, json={"error": {"message": "temporary"}}),
        ])
        with caplog.at_level("WARNING"):
            result = collect_stripe({"primary": "rk_test"}, since=SINCE)
        # Partial result kept, not discarded
        assert result is not None
        assert result["accounts"]["primary"]["charges_succeeded"] == 1
        assert result["accounts"]["primary"]["gross_cents"] == 1000
        # Warning surfaced
        assert any("partial results" in r.getMessage() for r in caplog.records)

    def test_first_page_failure_returns_none(self, respx_mock):
        """Failure on the very first page (no prior data) still returns None."""
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(503, json={"error": {"message": "down"}})
        )
        # Charges = None → treated as empty, account still returns with 0 charges
        result = collect_stripe({"primary": "rk_test"}, since=SINCE)
        assert result is not None
        assert result["accounts"]["primary"]["charges_succeeded"] == 0
        assert result["accounts"]["primary"]["gross_cents"] == 0

    def test_non_usd_charges_excluded_from_aggregates(self, respx_mock, caplog):
        """Non-USD charges are excluded from monthly/gross with a warning; USD kept."""
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "ch_usd", "status": "succeeded", "amount": 10000,
                 "amount_refunded": 500, "currency": "usd",
                 "created": int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_eur", "status": "succeeded", "amount": 50000,
                 "amount_refunded": 1000, "currency": "eur",
                 "created": int(datetime(2026, 3, 5, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_gbp", "status": "succeeded", "amount": 20000,
                 "amount_refunded": 0, "currency": "gbp",
                 "created": int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())},
            ]))
        )
        with caplog.at_level("WARNING"):
            result = collect_stripe({"primary": "rk_test"}, since=SINCE)
        acct = result["accounts"]["primary"]
        assert acct["currency"] == "usd"
        assert acct["charges_succeeded"] == 1  # only the USD charge
        assert acct["gross_cents"] == 10000
        assert acct["refunded_cents"] == 500  # non-USD refunds also excluded
        assert len(acct["monthly"]) == 1
        assert acct["monthly"][0]["month"] == "2026-03"
        # Warning fired mentioning both foreign currencies
        msgs = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any("eur" in m and "gbp" in m for m in msgs)

    def test_all_usd_no_warning(self, respx_mock, caplog):
        """When every charge is USD, no currency warning is emitted."""
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "ch_1", "status": "succeeded", "amount": 1000, "currency": "usd",
                 "created": int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp())},
            ]))
        )
        with caplog.at_level("WARNING"):
            collect_stripe({"primary": "rk_test"}, since=SINCE)
        assert not any("non-USD" in r.getMessage() for r in caplog.records)

    def test_monthly_rollup_groups_by_utc_month(self, respx_mock):
        respx_mock.get(f"{STRIPE}/products").mock(
            return_value=httpx.Response(200, json=_products_response([]))
        )
        respx_mock.get(f"{STRIPE}/charges").mock(
            return_value=httpx.Response(200, json=_paginated([
                {"id": "ch_1", "status": "succeeded", "amount": 1000, "currency": "usd",
                 "created": int(datetime(2026, 1, 15, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_2", "status": "succeeded", "amount": 2000, "currency": "usd",
                 "created": int(datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp())},
                {"id": "ch_3", "status": "succeeded", "amount": 3000, "currency": "usd",
                 "created": int(datetime(2026, 2, 1, tzinfo=timezone.utc).timestamp())},
            ]))
        )
        respx_mock.get(f"{STRIPE}/checkout/sessions").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        respx_mock.get(f"{STRIPE}/invoices").mock(
            return_value=httpx.Response(200, json=_paginated([]))
        )
        result = collect_stripe({"a": "rk"}, since=SINCE)
        monthly = {m["month"]: m for m in result["accounts"]["a"]["monthly"]}
        assert monthly["2026-01"]["count"] == 2
        assert monthly["2026-01"]["gross_cents"] == 3000
        assert monthly["2026-02"]["count"] == 1
        assert monthly["2026-02"]["gross_cents"] == 3000


# ---------------------------------------------------------------------------
# Store persistence
# ---------------------------------------------------------------------------

class TestProcessStripe:
    def test_writes_monthly_sessions_invoices_products(self, tmp_path):
        store_path = tmp_path / "analytics.xlsx"
        collected = {
            "accounts": {
                "primary": {
                    "currency": "usd",
                    "monthly": [
                        {"month": "2026-01", "count": 3, "gross_cents": 15000},
                        {"month": "2026-02", "count": 1, "gross_cents": 49900},
                    ],
                    "paid_sessions": [
                        {"id": "cs_1", "created": 1740000000, "amount_cents": 49900,
                         "currency": "usd", "metadata": {"courseName": "A"},
                         "customer_email": "x@y.com"},
                    ],
                    "paid_invoices": [
                        {"id": "in_1", "created": 1741000000,
                         "amount_paid_cents": 449500, "currency": "usd",
                         "lines": [{"description": "Course A", "amount_cents": 449500}]},
                    ],
                    "products": [
                        {"id": "prod_1", "name": "Course A", "active": True},
                    ],
                }
            }
        }
        sheets: dict = {}
        _process_stripe(collected, sheets, store_path, "2026-07-10 12:00:00")

        assert set(sheets.keys()) == {
            "stripe_monthly", "stripe_sessions", "stripe_invoices", "stripe_products",
        }
        assert len(sheets["stripe_monthly"]) == 2
        assert sheets["stripe_sessions"].iloc[0]["session_id"] == "cs_1"
        assert "courseName" in sheets["stripe_sessions"].iloc[0]["metadata_json"]
        assert sheets["stripe_invoices"].iloc[0]["invoice_id"] == "in_1"
        assert sheets["stripe_products"].iloc[0]["name"] == "Course A"

    def test_empty_accounts_writes_nothing(self, tmp_path):
        store_path = tmp_path / "analytics.xlsx"
        sheets: dict = {}
        _process_stripe({"accounts": {}}, sheets, store_path, "2026-07-10 12:00:00")
        assert sheets == {}

    def test_multi_account_writes_all(self, tmp_path):
        store_path = tmp_path / "analytics.xlsx"
        collected = {
            "accounts": {
                "a": {
                    "currency": "usd",
                    "monthly": [{"month": "2026-01", "count": 1, "gross_cents": 100}],
                    "paid_sessions": [], "paid_invoices": [], "products": [],
                },
                "b": {
                    "currency": "usd",
                    "monthly": [{"month": "2026-01", "count": 2, "gross_cents": 500}],
                    "paid_sessions": [], "paid_invoices": [], "products": [],
                },
            }
        }
        sheets: dict = {}
        _process_stripe(collected, sheets, store_path, "2026-07-10 12:00:00")
        rows = sheets["stripe_monthly"]
        assert set(rows["account"]) == {"a", "b"}
