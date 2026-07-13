from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso, _default_since

logger = logging.getLogger(__name__)

STRIPE_BASE = "https://api.stripe.com/v1"


def _all_pages(
    client: httpx.Client,
    auth: tuple[str, str],
    endpoint: str,
    params: dict[str, Any],
) -> list[dict] | None:
    """
    Paginate a Stripe list endpoint.

    Returns:
      - list of items on success (possibly empty)
      - partial list if a mid-pagination page fails (with a warning) so a
        transient 5xx on page 3 of 10 doesn't discard the first two pages
      - None only when the very first page fails
    """
    out: list[dict] = []
    starting_after: str | None = None
    while True:
        p = dict(params)
        if starting_after:
            p["starting_after"] = starting_after
        r = client.get(f"{STRIPE_BASE}/{endpoint}", auth=auth, params=p)
        if not r.is_success:
            logger.error(
                "Stripe %s failed: HTTP %s — %s",
                endpoint, r.status_code, r.text[:200],
            )
            if out:
                logger.warning(
                    "Stripe %s: returning %d partial results from earlier pages",
                    endpoint, len(out),
                )
                return out
            return None
        payload = r.json()
        batch = payload.get("data", [])
        out.extend(batch)
        if not payload.get("has_more") or not batch:
            break
        starting_after = batch[-1]["id"]
    return out


def _collect_one_account(
    label: str,
    token: str,
    since: datetime,
) -> dict[str, Any] | None:
    """Collect data for a single Stripe account (identified by user-chosen label)."""
    auth = (token, "")
    since_ts = int(since.timestamp())
    now = _utcnow()

    with httpx.Client(timeout=30) as client:
        products = _all_pages(client, auth, "products", {"limit": 100})
        if products is None:
            logger.error("Stripe [%s]: could not fetch products — aborting account", label)
            return None

        charges = _all_pages(
            client, auth, "charges",
            {"limit": 100, "created[gte]": since_ts},
        )
        sessions = _all_pages(
            client, auth, "checkout/sessions",
            {"limit": 100, "created[gte]": since_ts},
        )
        # Invoices need an extra scope — soft-fail
        invoices = _all_pages(
            client, auth, "invoices",
            {"limit": 100, "created[gte]": since_ts},
        )

    charges = charges or []
    sessions = sessions or []
    invoices = invoices or []

    succeeded = [c for c in charges if c.get("status") == "succeeded"]
    paid_sessions = [s for s in sessions if s.get("payment_status") == "paid"]
    paid_invoices = [i for i in invoices if i.get("status") == "paid"]

    # Aggregate in USD only. Multi-currency rollups would garble totals
    # (adding EUR cents to USD cents), so non-USD charges are excluded
    # from monthly/gross/refunded with a warning. Individual per-session
    # and per-invoice records preserve their own currency for downstream.
    def _is_usd(c: dict) -> bool:
        return (c.get("currency") or "usd").lower() == "usd"

    succeeded_usd = [c for c in succeeded if _is_usd(c)]
    non_usd = sorted({(c.get("currency") or "").lower() for c in succeeded if not _is_usd(c)})
    if non_usd:
        logger.warning(
            "Stripe [%s]: excluded %d non-USD succeeded charge(s) from aggregation "
            "(currencies: %s). USD-only until multi-currency rollup is supported.",
            label, len(succeeded) - len(succeeded_usd), ", ".join(non_usd),
        )

    # Monthly rollup by charge date (USD-only)
    monthly: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "gross_cents": 0})
    for c in succeeded_usd:
        m = datetime.fromtimestamp(c["created"], tz=timezone.utc).strftime("%Y-%m")
        monthly[m]["count"] += 1
        monthly[m]["gross_cents"] += c.get("amount", 0) or 0

    currency = "usd"

    gross_cents = sum((c.get("amount") or 0) for c in succeeded_usd)
    refunded_cents = sum((c.get("amount_refunded") or 0) for c in charges if _is_usd(c))

    # Slim per-session records — preserve metadata for downstream classification
    session_records = []
    for s in paid_sessions:
        session_records.append({
            "id": s["id"],
            "created": s.get("created"),
            "amount_cents": s.get("amount_total"),
            "currency": s.get("currency"),
            "metadata": s.get("metadata") or {},
            "customer_email": (s.get("customer_details") or {}).get("email"),
        })

    # Invoice records with line-item descriptions preserved.
    #
    # Payment signals — critical for downstream deduping. Two invoices can both
    # be `status="paid"` but only one actually moved money:
    #   - `attempt_count >= 1` — Stripe successfully charged via this invoice
    #   - `paid_out_of_band = True` — marked paid manually (bank transfer, etc.)
    # A "record invoice" auto-created for a checkout session that already paid
    # has `attempt_count == 0` and `paid_out_of_band = False` — no money moved
    # through the invoice; the checkout session is already counted elsewhere.
    # `collection_method` (charge_automatically / send_invoice) and
    # `billing_reason` (manual / subscription_create / …) preserved for extra
    # downstream filtering.
    invoice_records = []
    for i in paid_invoices:
        invoice_records.append({
            "id": i["id"],
            "created": i.get("created"),
            "amount_paid_cents": i.get("amount_paid"),
            "currency": i.get("currency"),
            "attempt_count": i.get("attempt_count", 0),
            "paid_out_of_band": bool(i.get("paid_out_of_band", False)),
            "collection_method": i.get("collection_method"),
            "billing_reason": i.get("billing_reason"),
            "lines": [
                {
                    "description": li.get("description"),
                    "amount_cents": li.get("amount"),
                }
                for li in (i.get("lines") or {}).get("data", [])
            ],
        })

    product_records = [
        {
            "id": p["id"],
            "name": p.get("name"),
            "active": p.get("active", False),
        }
        for p in products
    ]

    return {
        "label": label,
        "collected_at": _iso(now),
        "since": _iso(since),
        "currency": currency,
        "products": product_records,
        "charges_succeeded": len(succeeded_usd),
        "gross_cents": gross_cents,
        "refunded_cents": refunded_cents,
        "monthly": [
            {"month": m, "count": v["count"], "gross_cents": v["gross_cents"]}
            for m, v in sorted(monthly.items())
        ],
        "paid_sessions": session_records,
        "paid_invoices": invoice_records,
    }


def collect_stripe(
    tokens: dict[str, str],
    since: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Collect Stripe data (charges, checkout sessions, invoices, products) from
    one or more Stripe accounts.

    Args:
        tokens: mapping of user-chosen label → Stripe restricted key. Supports
            multi-account setups (e.g. `{"primary": "rk_...", "secondary": "rk_..."}`).
        since: earliest `created` timestamp to include. Defaults to 14 days back.

    Returns:
        A dict of the form::

            {
                "platform": "stripe",
                "collected_at": "...",
                "since": "...",
                "accounts": {label: <per-account data>, ...},
                "totals": {"gross_cents": int, "charges_succeeded": int},
            }

        or None if `tokens` is empty or every account failed.

    All classification (which sales count as which product) is intentionally
    left to downstream analysis — this collector preserves checkout metadata
    intact so any project can filter/group by its own rules.

    Currency: gross/monthly/refunded aggregates are USD-only. Non-USD
    succeeded charges are excluded from those rollups with a warning; the
    individual charge / session / invoice records preserve their own
    currency for downstream inspection.
    """
    if not tokens:
        return None
    if since is None:
        since = _default_since()

    accounts: dict[str, Any] = {}
    for label, token in tokens.items():
        if not token:
            logger.info("Stripe [%s]: no token — skipping", label)
            continue
        try:
            data = _collect_one_account(label, token, since)
            if data is None:
                continue
            accounts[label] = data
            logger.info(
                "Stripe [%s]: %d charges succeeded, %d paid sessions, %d paid invoices",
                label, data["charges_succeeded"],
                len(data["paid_sessions"]), len(data["paid_invoices"]),
            )
        except Exception as exc:
            logger.error("Stripe [%s] collection failed: %s", label, exc)

    if not accounts:
        return None

    totals = {
        "gross_cents": sum(a["gross_cents"] for a in accounts.values()),
        "charges_succeeded": sum(a["charges_succeeded"] for a in accounts.values()),
    }

    return {
        "platform": "stripe",
        "collected_at": _iso(_utcnow()),
        "since": _iso(since),
        "accounts": accounts,
        "totals": totals,
    }
