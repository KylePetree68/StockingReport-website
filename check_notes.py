"""
Validate water_notes.json before committing.

Checks:
  - file parses as JSON
  - every key (except _readme / other _ keys) matches a water in stocking_data_clean.json,
    either exactly or unambiguously via the same canonical matcher the generator uses
  - only known fields are present
  - `boats` is blank or one of the allowed values
  - reports which entries have content vs. are still empty stubs

Exit code 1 on any error so it can gate a commit.
"""
import json
import sys

from scraper import _resolve_by_canonical, BOAT_LABELS, WATER_NOTES_FILE

FIELDS = {"access_parking", "shoreline_ramps", "boats", "boats_source", "description"}


def main():
    try:
        with open(WATER_NOTES_FILE, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"ERROR: {WATER_NOTES_FILE} does not parse: {e}")
        return 1

    with open("stocking_data_clean.json", encoding="utf-8") as f:
        canonical = list(json.load(f).keys())

    notes = {k: v for k, v in raw.items() if not k.startswith("_")}
    errors = []
    warnings = []

    for key, val in notes.items():
        if not isinstance(val, dict):
            errors.append(f"'{key}': value must be an object")
            continue
        unknown = set(val) - FIELDS
        if unknown:
            errors.append(f"'{key}': unknown field(s) {sorted(unknown)}")
        for fld, x in val.items():
            if x is not None and not isinstance(x, str):
                errors.append(f"'{key}'.{fld}: must be a string")
        boats = (val.get("boats") or "").strip()
        if boats and boats not in BOAT_LABELS:
            errors.append(f"'{key}'.boats: '{boats}' not one of {sorted(BOAT_LABELS)}")
        if boats and not (val.get("boats_source") or "").strip():
            warnings.append(f"'{key}': boats set but boats_source is blank (cite where the rule came from)")

    resolved, unmatched = _resolve_by_canonical(notes, canonical)
    for key, targets in unmatched:
        reason = f"ambiguous, matches {targets}" if targets else "no matching stocked water"
        errors.append(f"'{key}': {reason}")
    for key in notes:
        if key not in canonical and key not in [k for k, _ in unmatched]:
            warnings.append(f"'{key}': attaches by fuzzy match only; consider using the exact canonical name")

    filled = [k for k, v in notes.items() if isinstance(v, dict) and any((x or "").strip() for x in v.values() if isinstance(x, str))]
    empty = [k for k in notes if k not in filled]

    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")
    print(f"\n{len(notes)} entries: {len(filled)} with content, {len(empty)} empty stubs.")
    if filled:
        print("  With content: " + ", ".join(filled))
    if errors:
        print(f"\n{len(errors)} error(s).")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
