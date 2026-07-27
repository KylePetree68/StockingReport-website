#!/usr/bin/env python3
"""Verify that auxiliary species data still matches canonical stocked-water names.

The Species Present section on each water page is fed by three display-name-keyed
sources (fishing-rules booklet, ArcGIS regs, consumption advisories) that are
fuzzy-matched onto the stocking data's canonical water names by
scraper._resolve_by_canonical(). When a source key stops matching -- e.g. NMDGF
renames a water, or a newly-added booklet entry has a typo -- the species simply
never appears, silently.

This script re-runs that exact matching logic and reports every key that fails to
attach. Keys listed in known_species_mismatches.json are expected (unstocked
"orphan" waters and generic river names that map to several segments) and only
produce a warning. Any OTHER unmatched key is unexpected: it is reported as an
error and the script exits non-zero, which fails the CI step and triggers
GitHub's failed-run email.

Run locally after editing water_species.json:  python check_species_matches.py
"""
import json
import os
import sys

# Reuse the production matching logic so this check can never drift from it.
from scraper import _resolve_by_canonical

STOCKING_FILE = "stocking_data.json"
ALLOWLIST_FILE = "known_species_mismatches.json"

# (label, path, extractor) for each auxiliary source. The extractor returns a
# {display_name: value} dict, mirroring how scraper loads each source.
def _plain(raw):
    return {k: v for k, v in raw.items() if not k.startswith("_")}

def _regs(raw):
    return raw.get("matched_waters", {})

SOURCES = [
    ("water_species.json", "water_species.json", _plain),
    ("matched_regulations.json", "matched_regulations.json", _regs),
    ("consumption_advisories.json", "consumption_advisories.json", _plain),
]


def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    canonical_names = list(_load(STOCKING_FILE).keys())
    allowlist = _load(ALLOWLIST_FILE) if os.path.exists(ALLOWLIST_FILE) else {}

    total_unexpected = 0
    total_expected = 0
    stale_allow = 0

    for label, path, extract in SOURCES:
        if not os.path.exists(path):
            continue
        source = extract(_load(path))
        _resolved, unmatched = _resolve_by_canonical(source, canonical_names)
        known = set(allowlist.get(label, []))
        unmatched_keys = {key for key, _targets in unmatched}

        for key, targets in unmatched:
            reason = f"ambiguous -> {targets}" if targets else "no matching stocked water"
            if key in known:
                total_expected += 1
                print(f"::warning title=Species match (known)::{label}: '{key}' not attached ({reason}).")
            else:
                total_unexpected += 1
                print(f"::error title=Species match (NEW)::{label}: '{key}' does not attach to any stocked water ({reason}). "
                      f"Fix the name in {path} to match a water in {STOCKING_FILE}, or add it to {ALLOWLIST_FILE} if intentional.")

        # Allowlist entries that now match (or were removed) are stale -- flag so the
        # list gets tidied, but don't fail the build over it.
        for key in sorted(known - unmatched_keys):
            stale_allow += 1
            print(f"::warning title=Stale allowlist::{label}: '{key}' is in {ALLOWLIST_FILE} but now matches (or was removed). "
                  f"Remove it from the allowlist.")

    print(
        f"\nSpecies-match check: {total_unexpected} unexpected, "
        f"{total_expected} known-skipped, {stale_allow} stale allowlist entr"
        f"{'y' if stale_allow == 1 else 'ies'}."
    )
    if total_unexpected:
        print("FAILED: new unmatched species-source name(s) found (see ::error:: above).")
        return 1
    print("OK: all species-source names match or are known-skipped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
