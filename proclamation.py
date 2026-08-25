#!/usr/bin/env python3
"""
Shared accessor for the current year's NM fishing proclamation.

The booklet URL lives in exactly one place -- proclamation.json -- and every
consumer reads it from here: the static pages (via update_proclamation.py),
the generated water pages (via scraper.py's {{PROCLAMATION_URL}} placeholder),
and the consumption-advisory deep links.
"""

import json
import os
import re

CONFIG_FILE = "proclamation.json"

# Used when proclamation.json is missing or its pdf_url is blank, so a bad
# config degrades to the NMDOW publications index instead of an empty href.
FALLBACK_PUBLICATIONS_URL = "https://wildlife.dgf.nm.gov/home/publications/"


def _derive_season(pdf_url):
    """Pull '2026-2027' out of a filename like '2026-2027-FISH-RIB_Online.pdf'."""
    m = re.search(r"(\d{4})[-_](\d{4})", os.path.basename(pdf_url or ""))
    if not m:
        return ""
    return "%s–%s" % (m.group(1), m.group(2))   # en dash


def load(config_file=CONFIG_FILE):
    """
    Return (pdf_url, season, publications_url).

    pdf_url falls back to the publications index, so callers can always link
    somewhere useful. season is "" when it can't be derived and isn't set.
    """
    cfg = {}
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (OSError, ValueError) as e:
            print("Warning: could not read %s: %s" % (config_file, e))

    publications_url = cfg.get("publications_url") or FALLBACK_PUBLICATIONS_URL
    pdf_url = cfg.get("pdf_url") or publications_url
    season = cfg.get("season") or _derive_season(cfg.get("pdf_url"))
    return pdf_url, season, publications_url
