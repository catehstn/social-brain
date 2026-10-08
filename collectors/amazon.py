from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx

from collectors._helpers import _utcnow, _iso

logger = logging.getLogger(__name__)


_AMAZON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class AmazonBlockedError(Exception):
    """Amazon served a bot-check page instead of the product page.

    Classified by its own exception so the collect loop can distinguish
    'this marketplace is blocking us — skip the rest' from the ordinary
    parse failures of `_scrape_amazon_asin` (which also raise `ValueError`
    for bad numeric substrings), without the loop having to grep the
    exception message for 'CAPTCHA'/'Bot'.
    """


# Marker found on Amazon's own bot-check page. Specific — a product page
# whose copy happens to mention "captcha" (a book about CAPTCHAs, a
# security-related product listing) will not carry this element.
_AMAZON_CAPTCHA_MARKER = 'id="captchacharacters"'


def _scrape_amazon_asin(client: httpx.Client, asin: str, marketplace: str) -> dict[str, Any]:
    """Fetch a single Amazon product page and extract public metrics."""
    r = client.get(f"https://www.{marketplace}/dp/{asin}", follow_redirects=True)
    # CAPTCHA marker check runs BEFORE raise_for_status: Amazon sometimes
    # serves the bot-check page with 503, and the generic http-error arm
    # would otherwise retry the next ASIN against the same blocked IP.
    html = r.text
    if _AMAZON_CAPTCHA_MARKER in html:
        raise AmazonBlockedError(f"Bot/CAPTCHA page returned by {marketplace}")
    r.raise_for_status()

    title_m = re.search(r'id="productTitle"[^>]*>\s*([^<]+)', html)
    title = title_m.group(1).strip() if title_m else None

    rating_m = re.search(r'([\d.]+) out of 5 stars', html)
    rating = float(rating_m.group(1)) if rating_m else None

    reviews_m = re.search(
        r'acrCustomerReviewText[^>]+aria-label="([\d,]+) Reviews"'
        r'|(?:^|[^\d])([\d,]+)\s+(?:global\s+)?ratings',
        html, re.IGNORECASE,
    )
    if reviews_m:
        raw = next(g for g in reviews_m.groups() if g)
        reviews = int(raw.replace(",", ""))
    else:
        reviews = None

    # Best Sellers Rank:
    #   amazon.com:    "#1,234 in Books" / "#159,450 in Audible Books"
    #   amazon.co.uk:  "Best Sellers Rank: </span> 419,623 in Kindle Store"
    rank_m = (
        re.search(r"#([\d,]+)\s+in\s+(?:Books|Kindle|Audible)", html)
        or re.search(r"Best Sellers Rank:[^>]*>\s*([\d,]+)\s+in\s+", html)
    )
    rank = int(rank_m.group(1).replace(",", "")) if rank_m else None

    return {
        "asin": asin,
        "title": title,
        "rating": rating,
        "reviews": reviews,
        "best_sellers_rank": rank,
        "url": f"https://www.{marketplace}/dp/{asin}",
    }


def collect_amazon(
    asins: list[str],
    marketplaces: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    Collect public book metrics (sales rank, rating, review count) from
    Amazon product pages across one or more marketplaces.
    No authentication required.
    """
    if not asins:
        logger.info("Amazon: no ASINs configured — skipping")
        return None

    if not marketplaces:
        marketplaces = ["amazon.com", "amazon.co.uk"]

    results: dict[str, list] = {}
    with httpx.Client(timeout=30, headers=_AMAZON_HEADERS) as client:
        for marketplace in marketplaces:
            books = []
            blocked = False
            for i, asin in enumerate(asins):
                if i > 0:
                    time.sleep(1)
                try:
                    book = _scrape_amazon_asin(client, asin, marketplace)
                    books.append(book)
                    logger.info(
                        "Amazon [%s/%s]: rank=#%s, rating=%s, reviews=%s — %s",
                        marketplace, asin, book["best_sellers_rank"],
                        book["rating"], book["reviews"],
                        book["title"] or "(no title)",
                    )
                except AmazonBlockedError:
                    logger.warning(
                        "Amazon [%s/%s]: bot detection triggered — dropping marketplace (%d earlier ASINs discarded)",
                        marketplace, asin, len(books),
                    )
                    blocked = True
                    break
                except Exception as exc:
                    logger.warning("Amazon [%s/%s] failed: %s", marketplace, asin, exc)
            # Discard anything collected before the block: the warning says
            # "skipping marketplace", so the result must not silently include
            # a truncated prefix of ASINs that happened to come in first.
            if books and not blocked:
                results[marketplace] = books

    if not results:
        return None

    return {
        "platform": "amazon",
        "collected_at": _iso(_utcnow()),
        "by_marketplace": results,
    }
