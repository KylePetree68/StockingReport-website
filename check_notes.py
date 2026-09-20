"""
Validate water_notes.json and water_authority.json before committing.

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

from scraper import (_resolve_by_canonical, BOAT_LABELS, WATER_NOTES_FILE,
                     WATER_AUTHORITY_FILE, AUTHORITY_CATEGORY_LABELS)

FIELDS = {"access_parking", "shoreline_ramps", "boats", "boats_source", "description"}
AUTHORITY_FIELDS = {"authority", "category", "unit", "url", "notes", "confidence", "source"}
AUTHORITY_CATEGORIES = set(AUTHORITY_CATEGORY_LABELS) | {"unknown"}


def check_authority(canonical):
    """Validate water_authority.json. Returns (errors, warnings)."""
    errors, warnings = [], []
    try:
        with open(WATER_AUTHORITY_FILE, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return errors, [f"{WATER_AUTHORITY_FILE} not present (optional)"]
    except Exception as e:
        return [f"{WATER_AUTHORITY_FILE} does not parse: {e}"], warnings

    entries = {k: v for k, v in raw.items() if not k.startswith("_")}
    for key, val in entries.items():
        if not isinstance(val, dict):
            errors.append(f"authority '{key}': value must be an object")
            continue
        missing = AUTHORITY_FIELDS - set(val)
        unknown = set(val) - AUTHORITY_FIELDS
        if missing:
            errors.append(f"authority '{key}': missing field(s) {sorted(missing)}")
        if unknown:
            errors.append(f"authority '{key}': unknown field(s) {sorted(unknown)}")
        if val.get("category") not in AUTHORITY_CATEGORIES:
            errors.append(f"authority '{key}': category '{val.get('category')}' not one of {sorted(AUTHORITY_CATEGORIES)}")
        if val.get("confidence") not in ("high", "medium", "low"):
            errors.append(f"authority '{key}': confidence must be high|medium|low")
        url = val.get("url") or ""
        if url and not url.startswith("https://"):
            warnings.append(f"authority '{key}': url is not https ({url})")
        if val.get("confidence") in ("high", "medium") and val.get("category") == "unknown":
            errors.append(f"authority '{key}': category unknown cannot be high/medium confidence")
        if val.get("category") == "mixed" and not (val.get("notes") or "").strip():
            warnings.append(f"authority '{key}': mixed ownership with no notes saying which stretch is public")

    _, unmatched = _resolve_by_canonical(entries, canonical)
    for key, targets in unmatched:
        reason = f"ambiguous, matches {targets}" if targets else "no matching stocked water"
        errors.append(f"authority '{key}': {reason}")
    uncovered = [c for c in canonical if c not in entries]
    if uncovered:
        warnings.append(f"authority: {len(uncovered)} water(s) have no entry: {uncovered[:8]}{' ...' if len(uncovered) > 8 else ''}")

    by_conf = {c: sum(1 for v in entries.values() if isinstance(v, dict) and v.get("confidence") == c) for c in ("high", "medium", "low")}
    print(f"{len(entries)} authority entries: {by_conf['high']} high, {by_conf['medium']} medium, {by_conf['low']} low (low is not rendered).")
    low = [k for k, v in entries.items() if isinstance(v, dict) and v.get("confidence") == "low"]
    if low:
        print("  Low confidence (verify): " + ", ".join(low))
    return errors, warnings


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

    a_errors, a_warnings = check_authority(canonical)
    errors += a_errors
    warnings += a_warnings

    # boat_rules.json: keys must resolve, rule must be a known enum value
    try:
        with open("boat_rules.json", encoding="utf-8") as f:
            braw = json.load(f)
        brules = {k: v for k, v in braw.items() if not k.startswith("_")}
        for key, val in brules.items():
            if not isinstance(val, dict) or not (val.get("text") or "").strip():
                errors.append(f"boat_rules '{key}': needs a non-empty text")
            elif val.get("rule") not in BOAT_LABELS:
                errors.append(f"boat_rules '{key}': rule '{val.get('rule')}' not one of {sorted(BOAT_LABELS)}")
        _, b_unmatched = _resolve_by_canonical(brules, canonical)
        for key, targets in b_unmatched:
            errors.append(f"boat_rules '{key}': " + (f"ambiguous, matches {targets}" if targets else "no matching stocked water"))
        print(f"{len(brules)} boat-rule entries.")
    except FileNotFoundError:
        pass
    except Exception as e:
        errors.append(f"boat_rules.json does not parse: {e}")

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
