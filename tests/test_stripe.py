"""
Tests for collectors/stripe.py and its wiring.
All HTTP is mocked with respx; no real Stripe key needed.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pandas as pd
import pytest
import respx

from collectors.stripe import collect_stripe
from collectors._dispatch import _resolve_stripe_tokens
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
