from __future__ import annotations

import email as _email
import logging
import re
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from collectors._helpers import _utcnow, _iso

logger = logging.getLogger(__name__)


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.texts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        self._skip = tag in ("style", "script")

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            t = data.strip()
            if t:
                self.texts.append(t)


def _parse_oreilly_eml(path: Path) -> dict[str, Any] | None:
    """
    Parse a single O'Reilly Payment Remittance Advice .eml file.
    Extracts payment date, amount, currency, and line items.
    """
    with path.open("rb") as f:
        msg = _email.message_from_binary_file(f)

    html_body = ""
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            html_body = part.get_payload(decode=True).decode("utf-8", errors="replace")
            break

    if not html_body:
        logger.warning("O'Reilly: no HTML body found in %s", path.name)
        return None

    extractor = _TextExtractor()
    extractor.feed(html_body)
    texts = extractor.texts

    def _after(label: str) -> str | None:
        for i, t in enumerate(texts):
            if t.strip() == label and i + 1 < len(texts):
                return texts[i + 1].strip()
        return None

    payment_date_str = _after("Payment Date")
    amount_str = _after("Payment Amount")
    currency = _after("Payment Currency")
    doc_number = _after("Paper Document Number")

    if not payment_date_str or not amount_str:
        logger.warning("O'Reilly: could not parse key fields from %s", path.name)
        return None

    try:
        payment_date = datetime.strptime(payment_date_str, "%b %d, %Y").strftime("%Y-%m-%d")
        amount = float(amount_str.replace(",", ""))
    except (ValueError, AttributeError) as exc:
        logger.warning("O'Reilly: failed to parse values from %s: %s", path.name, exc)
        return None

    # Extract remittance line items. Rows have 8 columns:
    # Doc Ref | Doc Date | Description | Doc Amount | Currency |
    # Amount Withheld | Discount Taken | Amount Paid
    # "Amount Withheld" may be empty (non-US authors with no withholding tax)
    # producing a 7-token row. We detect which case applies per-row.
    line_items = []
    try:
        # Find the first data row by locating a doc-ref pattern (e.g. "AP-...")
        data_start = next(
            (i for i, t in enumerate(texts) if re.match(r"^[A-Z]+-\d+", t)),
            None,
        )
        if data_start is not None:
            i = data_start
            while i + 6 < len(texts):
                if not re.match(r"^[A-Z]+-\d+", texts[i]):
                    break
                # Peek ahead: if texts[i+5] looks like a currency code the
                # withheld column is present (8-token row), otherwise absent (7-token).
                peek = texts[i:i + 8]
                has_withheld = len(peek) >= 8 and re.match(r"^[A-Z]{3}$", peek[4]) and re.match(r"^\d", peek[5])
                row_len = 8 if has_withheld else 7
                row = texts[i:i + row_len]
                if len(row) < row_len:
                    break
                try:
                    if has_withheld:
                        doc_ref, doc_date, desc, doc_amt, cur, withheld, discount, amt_paid = row
                    else:
                        doc_ref, doc_date, desc, doc_amt, cur, discount, amt_paid = row
                        withheld = "0"
                    line_items.append({
                        "doc_ref": doc_ref,
                        "doc_date": doc_date,
                        "description": desc,
                        "doc_amount": float(doc_amt.replace(",", "")),
                        "currency": cur,
                        "amount_withheld": float(withheld.replace(",", "").lstrip(".") or "0"),
                        "discount": float(discount.replace(",", "").lstrip(".") or "0"),
                        "amount_paid": float(amt_paid.replace(",", "")),
                    })
                except (ValueError, IndexError):
                    break
                i += row_len
    except Exception as exc:
        logger.warning("O'Reilly: could not parse line items from %s: %s", path.name, exc)

    return {
        "payment_date": payment_date,
        "doc_number": doc_number,
        "amount": amount,
        "currency": currency,
        "line_items": line_items,
        "source_file": path.name,
    }


def collect_oreilly(oreilly_drops_dir: str | Path = "oreilly_drops") -> dict[str, Any] | None:
    """
    Parse all O'Reilly Payment Remittance Advice .eml files from
    oreilly_drops/ and return the full payment history sorted by date.
    """
    drops_path = Path(oreilly_drops_dir)
    eml_files = sorted(drops_path.glob("*.eml"), key=lambda p: p.stat().st_mtime)

    if not eml_files:
        logger.info("O'Reilly: no .eml files found in %s — skipping", drops_path)
        return None

    payments = []
    for path in eml_files:
        parsed = _parse_oreilly_eml(path)
        if parsed:
            payments.append(parsed)
        else:
            logger.warning("O'Reilly: skipping unparseable file %s", path.name)

    if not payments:
        return None

    payments.sort(key=lambda p: p["payment_date"])

    total_paid = sum(p["amount"] for p in payments)
    currencies = list({p["currency"] for p in payments if p["currency"]})

    logger.info(
        "O'Reilly: parsed %d payment(s), total %s %.2f",
        len(payments), currencies[0] if len(currencies) == 1 else str(currencies), total_paid,
    )

    return {
        "platform": "oreilly",
        "collected_at": _iso(_utcnow()),
        "payments": payments,
        "total_paid": total_paid,
        "currencies": currencies,
        "payment_count": len(payments),
    }
