#!/usr/bin/env python3
"""
Remove duplicate records where both local and NMDGF URLs exist.
Prefer NMDGF URLs for credibility.
"""

import json
from collections import defaultdict

INPUT_FILE = "stocking_data.json"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data_before_dedup.json"

def get_filename_from_url(url):
    """Extract filename from URL."""
    if '/public/reports/' in url:
        return url.split('/')[-1]
    elif 'wildlife.dgf.nm.gov' in url and 'stocking-report' in url:
        # Extract from NMDGF URL like .../stocking-report-10-31-25/?wpdmdl=...
        parts = url.split('/')
        for p in parts:
            if 'stocking-report' in p:
                return p.split('?')[0] + '.pdf'
    return None

def deduplicate_urls():
    """Remove duplicate records, preferring NMDGF URLs over local ones."""
    print("=" * 80)
    print("REMOVING DUPLICATE URLS")
    print("=" * 80)

    # Load data
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"\nLoaded {len(data)} water bodies")

    # Backup
    with open(BACKUP_FILE, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Created backup: {BACKUP_FILE}")

    removed_count = 0

    # Process each water body
    for water_name, water_data in data.items():
        records = water_data['records']

        # Group records by filename
        filename_groups = defaultdict(list)
        other_records = []

        for i, record in enumerate(records):
            url = record.get('reportUrl', '')
            filename = get_filename_from_url(url)

            if filename:
                filename_groups[filename].append((i, record, url))
            else:
                other_records.append(record)

        # For each filename group, prefer NMDGF URL
        new_records = []
        for filename, items in filename_groups.items():
            if len(items) > 1:
                # Multiple records for same filename
                # Prefer NMDGF URL
                nmdgf_records = [(i, rec, url) for i, rec, url in items if 'wildlife.dgf.nm.gov' in url]
                local_records = [(i, rec, url) for i, rec, url in items if '/public/reports/' in url]

                if nmdgf_records:
                    # Use NMDGF version
                    new_records.append(nmdgf_records[0][1])
                    removed_count += len(items) - 1
                    if local_records:
                        print(f"  {water_name}: Removed local URL for {filename}, kept NMDGF URL")
                else:
                    # No NMDGF version, keep local
                    new_records.append(items[0][1])
                    if len(items) > 1:
                        removed_count += len(items) - 1
            else:
                # Single record, keep it
                new_records.append(items[0][1])

        # Add back records without identifiable URLs
        new_records.extend(other_records)

        # Sort by date
        new_records.sort(key=lambda x: x['date'], reverse=True)
        water_data['records'] = new_records

    print(f"\nRemoved {removed_count} duplicate records")

    # Save
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Saved: {OUTPUT_FILE}")
    print("\n" + "=" * 80)
    print("DEDUPLICATION COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    deduplicate_urls()
