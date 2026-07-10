from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from collectors.mastodon import collect_mastodon
from collectors.bluesky import collect_bluesky
from collectors.buttondown import collect_buttondown
from collectors.jetpack import collect_jetpack
from collectors.linkedin import collect_linkedin
from collectors.linkedin_api import collect_linkedin_api
from collectors.substack import collect_substack
from collectors.vercel import collect_vercel
from collectors.amazon import collect_amazon
from collectors.upcoming import collect_upcoming
from collectors.mentions import collect_mentions
from collectors.goatcounter import collect_goatcounter
from collectors.oreilly import collect_oreilly
from collectors.calendly import collect_calendly
from collectors.stripe import collect_stripe

logger = logging.getLogger(__name__)


def _resolve_stripe_tokens(config: dict) -> dict[str, str]:
    """
    Extract Stripe tokens from config. Supports two shapes:

    1. `stripe_tokens: {label: key, ...}` — dict of user-labelled accounts.
    2. Flat keys `stripe_token_<label>: key` — any config key matching this
       prefix is treated as one Stripe account (label is the suffix).

    Empty tokens are dropped. Returns {} if no tokens configured.
    """
    tokens: dict[str, str] = {}
    dict_form = config.get("stripe_tokens") or {}
    if isinstance(dict_form, dict):
        for label, key in dict_form.items():
            if key:
                tokens[str(label)] = str(key)
    for k, v in config.items():
        if not isinstance(k, str) or not k.startswith("stripe_token_"):
            continue
        label = k[len("stripe_token_"):]
        if label and v:
            tokens[label] = str(v)
    return tokens


def _stripe_since(config: dict, since: datetime | None) -> datetime | None:
    """
    Resolve the Stripe lookback window.

    An explicit global `since` (e.g. from ``--months``) always wins. Otherwise
    use ``stripe_since_days`` from config (default 60) so month-to-date and
    recent activity are covered — the collector's own 14-day default is too
    short for monthly rollups, and Stripe history is cheap to over-fetch.

    A non-positive ``stripe_since_days`` falls back to the collector's internal
    default (returns None).
    """
    if since is not None:
        return since
    days = config.get("stripe_since_days", 60)
    try:
        days = int(days)
    except (TypeError, ValueError):
        days = 60
    if days <= 0:
        return None
    return datetime.now(timezone.utc) - timedelta(days=days)


PLATFORM_COLLECTORS = {
    "mastodon": "collect_mastodon",
    "bluesky": "collect_bluesky",
    "buttondown": "collect_buttondown",
    "jetpack": "collect_jetpack",
    "linkedin": "collect_linkedin",
    "substack": "collect_substack",
    "vercel": "collect_vercel",
    "amazon": "collect_amazon",
    "upcoming": "collect_upcoming",
    "mentions": "collect_mentions",
    "goatcounter": "collect_goatcounter",
    "oreilly": "collect_oreilly",
    "calendly": "collect_calendly",
    "stripe": "collect_stripe",
}


def collect_all(
    config: dict,
    platform: str | None = None,
    since: datetime | None = None,
) -> dict[str, Any]:
    """
    Run all (or a single) collector and return a dict keyed by platform name.
    `since` overrides the default lookback window for all date-based collectors.
    Failures are logged but do not raise.
    """
    results: dict[str, Any] = {}

    def _run(name: str) -> None:
        if name == "mastodon":
            instance = config.get("mastodon_instance", "")
            handle = config.get("mastodon_handle", "")
            if not instance or not handle:
                logger.info("Mastodon: mastodon_instance or mastodon_handle not configured — skipping")
                return
            data = collect_mastodon(
                instance,
                handle,
                since=since,
                access_token=config.get("mastodon_access_token", ""),
            )
        elif name == "bluesky":
            handle = config.get("bluesky_handle", "")
            if not handle:
                logger.info("Bluesky: bluesky_handle not configured — skipping")
                return
            data = collect_bluesky(
                handle,
                since=since,
                app_password=config.get("bluesky_app_password", ""),
            )
        elif name == "buttondown":
            api_key = config.get("buttondown_api_key", "")
            if not api_key:
                logger.info("Buttondown: buttondown_api_key not configured — skipping")
                return
            data = collect_buttondown(api_key, since=since)
        elif name == "jetpack":
            jetpack_site = config.get("jetpack_site", "")
            jetpack_token = config.get("jetpack_access_token", "")
            if not jetpack_site or not jetpack_token:
                logger.info("Jetpack: jetpack_site or jetpack_access_token not configured — skipping")
                return
            data = collect_jetpack(
                jetpack_site, jetpack_token, since=since,
                client_id=config.get("jetpack_client_id", ""),
                client_secret=config.get("jetpack_client_secret", ""),
                username=config.get("jetpack_username", ""),
            )
        elif name == "linkedin":
            li_token = config.get("linkedin_access_token", "")
            if li_token:
                data = collect_linkedin_api(li_token, since=since)
            else:
                data = collect_linkedin()
        elif name == "substack":
            data = collect_substack()
        elif name == "amazon":
            asins = config.get("amazon_asins", [])
            if not asins:
                logger.info("Amazon: amazon_asins not configured — skipping")
                return
            data = collect_amazon(
                asins,
                marketplaces=config.get("amazon_marketplaces") or ["amazon.com", "amazon.co.uk"],
            )
        elif name == "vercel":
            vercel_token = config.get("vercel_token", "")
            vercel_project_id = config.get("vercel_project_id", "")
            if not vercel_token or not vercel_project_id:
                logger.info("Vercel: vercel_token or vercel_project_id not configured — skipping")
                return
            data = collect_vercel(
                vercel_token,
                vercel_project_id,
                team_id=config.get("vercel_team_id") or None,
                since=since,
            )
        elif name == "upcoming":
            jetpack_site = config.get("jetpack_site", "")
            jetpack_token = config.get("jetpack_access_token", "")
            buttondown_api_key = config.get("buttondown_api_key", "")
            buffer_token = config.get("buffer_token", "")
            if not any([jetpack_site and jetpack_token, buttondown_api_key, buffer_token]):
                logger.info("Upcoming: no sources configured — skipping")
                return
            data = collect_upcoming(
                jetpack_site=jetpack_site,
                jetpack_access_token=jetpack_token,
                buttondown_api_key=buttondown_api_key,
                buffer_token=buffer_token,
            )
        elif name == "mentions":
            domains = config.get("monitored_domains", [])
            if not domains:
                logger.info("Mentions: monitored_domains not configured — skipping")
                return
            data = collect_mentions(
                domains=domains,
                since=since,
                mastodon_instance=config.get("mastodon_instance", ""),
                mastodon_access_token=config.get("mastodon_access_token", ""),
                bluesky_handle=config.get("bluesky_handle", ""),
                bluesky_app_password=config.get("bluesky_app_password", ""),
                gsc_credentials_file=config.get("gsc_credentials_file", ""),
            )
        elif name == "goatcounter":
            gc_site = config.get("goatcounter_site", "")
            gc_token = config.get("goatcounter_token", "")
            if not gc_site or not gc_token:
                logger.info("GoatCounter: goatcounter_site or goatcounter_token not configured — skipping")
                return
            data = collect_goatcounter(gc_site, gc_token, since=since)
        elif name == "oreilly":
            data = collect_oreilly()
        elif name == "calendly":
            calendly_token = config.get("calendly_token", "")
            if not calendly_token:
                logger.info("Calendly: calendly_token not configured — skipping")
                return
            data = collect_calendly(calendly_token, since=since, lead_gen_event=config.get("calendly_lead_gen_event") or None)
        elif name == "stripe":
            tokens = _resolve_stripe_tokens(config)
            if not tokens:
                logger.info("Stripe: no tokens configured — skipping")
                return
            data = collect_stripe(tokens, since=_stripe_since(config, since))
        else:
            logger.error("Unknown platform: %s", name)
            return

        if data is not None:
            results[name] = data

    if platform:
        _run(platform)
    else:
        for name in PLATFORM_COLLECTORS:
            _run(name)

    return results
