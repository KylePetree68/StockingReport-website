#!/usr/bin/env python3
"""
Clean up the stocking data by:
1. Merging duplicate/similar water body names
2. Removing malformed entries with hatchery names in water names
3. Creating a clean master JSON file
"""

import json
import re
from collections import defaultdict

INPUT_FILE = "stocking_data.json"
OUTPUT_FILE = "stocking_data_clean.json"
BACKUP_FILE = "stocking_data_clean.json.bak"

def normalize_name(name):
    """Normalize a water body name for comparison."""
    # Convert to lowercase
    n = name.lower().strip()

    # Remove trailing punctuation issues
    n = re.sub(r'\s*\)\s*$', ')', n)  # Fix spacing before )
    n = re.sub(r'\s*\(\s*', '(', n)   # Fix spacing after (

    # Add missing closing parenthesis if there's an unmatched opening one
    open_count = n.count('(')
    close_count = n.count(')')
    if open_count > close_count:
        n = n + ')' * (open_count - close_count)

    # Standardize spacing
    n = ' '.join(n.split())

    return n

def is_malformed(name):
    """Check if a water name looks malformed (has hatchery fragments)."""
    name_lower = name.lower()

    # Check for hatchery keywords that shouldn't be in water names
    bad_keywords = [
        'hatchery', 'facility', 'lisboa springs', 'red river trout',
        'rock lake trout', 'los ojos', 'seven springs'
    ]

    for keyword in bad_keywords:
        if keyword in name_lower:
            return True

    # Check for incomplete parentheses
    if name.count('(') != name.count(')'):
        return True

    # Check if name ends with weird fragments
    if name.endswith(('Beach)', 'State Park)', 'Quality)')):
        # This is actually OK - proper ending
        pass
    elif re.search(r'\)\s+[A-Z]', name):
        # Parenthesis followed by capitalized word - likely malformed
        return True

    return False

def find_best_name(variations):
    """From a list of name variations, pick the best one."""
    # Prefer names that:
    # 1. End with proper closing parenthesis
    # 2. Are longer (more complete)
    # 3. Don't have trailing fragments

    scored = []
    for name in variations:
        score = 0

        # Prefer names ending with )
        if name.endswith(')'):
            score += 10

        # Prefer longer names
        score += len(name)

        # Prefer names without weird capitalization mid-string
        if not re.search(r'\s[A-Z][a-z]+\s[A-Z][a-z]+\)$', name):
            score += 5

        scored.append((score, name))

    # Return highest scoring name
    return max(scored)[1]

def merge_records(record_lists):
    """Merge multiple lists of records, removing duplicates."""
    seen = set()
    merged = []

    for records in record_lists:
        for rec in records:
            # Create a unique key for deduplication
            key = json.dumps(rec, sort_keys=True)
            if key not in seen:
                seen.add(key)
                merged.append(rec)

    # Sort by date, newest first
    merged.sort(key=lambda x: x['date'], reverse=True)
    return merged

def cleanup_data():
    """Main cleanup function."""
    print("=" * 80)
    print("CLEANING UP STOCKING DATA")
    print("=" * 80)

    # Load data
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"\nOriginal: {len(data)} water bodies")

    # Step 1: Group ALL names first (including malformed ones)
    print("\nStep 1: Grouping similar names (including malformed)...")
    grouped = defaultdict(list)

    for name in data.keys():
        normalized = normalize_name(name)
        grouped[normalized].append(name)

    # Step 2: Merge duplicates and handle malformed entries
    print("\nStep 2: Merging duplicates and rescuing malformed data...")
    final_data = {}
    merge_count = 0
    malformed_rescued = []
    malformed_discarded = []

    for normalized, variations in grouped.items():
        if len(variations) > 1:
            print(f"\n  Merging {len(variations)} variations:")
            for v in variations:
                malformed_status = " [MALFORMED]" if is_malformed(v) else ""
                print(f"    - \"{v}\" ({len(data[v]['records'])} records){malformed_status}")

            # Pick best name (prefer non-malformed)
            non_malformed = [v for v in variations if not is_malformed(v)]
            if non_malformed:
                best_name = find_best_name(non_malformed)
            else:
                # All are malformed, skip this group entirely
                print(f"  -> All variations are malformed, discarding")
                malformed_discarded.extend(variations)
                continue

            print(f"  -> Using: \"{best_name}\"")

            # Merge all records (including from malformed entries)
            all_records = [data[v]['records'] for v in variations]
            merged_records = merge_records(all_records)

            # Track which malformed entries had their data rescued
            for v in variations:
                if is_malformed(v) and v != best_name:
                    malformed_rescued.append(v)
                    print(f"  -> Rescued {len(data[v]['records'])} records from \"{v}\"")

            # Use coords from any variation that has them (prefer non-malformed)
            coords = None
            for v in non_malformed:
                if data[v].get('coords'):
                    coords = data[v]['coords']
                    break
            if not coords:  # Try malformed if no coords found
                for v in variations:
                    if data[v].get('coords'):
                        coords = data[v]['coords']
                        break

            final_data[best_name] = {
                'records': merged_records,
                'coords': coords
            }

            merge_count += 1
            print(f"  -> Total: {len(merged_records)} unique records")
        else:
            # Single entry - check if it's malformed
            name = variations[0]
            if is_malformed(name):
                print(f"\n  Discarding isolated malformed entry: \"{name}\"")
                malformed_discarded.append(name)
            else:
                # No duplicates, just copy
                final_data[name] = data[name]

    print(f"\nMerged {merge_count} sets of duplicates")
    print(f"Malformed entries rescued: {len(malformed_rescued)}")
    print(f"Malformed entries discarded: {len(malformed_discarded)}")
    print(f"Final count: {len(final_data)} water bodies")

    # Calculate total records
    total_records = sum(len(v['records']) for v in final_data.values())
    print(f"Total records: {total_records}")

    # Save cleaned data
    print(f"\nSaving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(final_data, f, indent=4)

    print("\n" + "=" * 80)
    print("CLEANUP COMPLETE!")
    print("=" * 80)
    print(f"\nBefore: {len(data)} water bodies")
    print(f"After:  {len(final_data)} water bodies")
    print(f"Removed: {len(data) - len(final_data)} duplicates/malformed")
    print(f"  - Rescued and merged: {len(malformed_rescued)} malformed entries")
    print(f"  - Discarded: {len(malformed_discarded)} isolated malformed entries")

    return final_data

if __name__ == "__main__":
    cleanup_data()
