#!/usr/bin/env python3
"""
Fix PDF links in stocking data to point to hosted copies.
"""

import json

INPUT_FILE = "stocking_data.json"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data_before_pdf_fix.json"

def fix_pdf_links():
    """Update local:// URLs to /public/reports/ URLs."""
    print("=" * 80)
    print("FIXING PDF LINKS")
    print("=" * 80)

    # Load data
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"\nLoaded {len(data)} water bodies")

    # Backup original
    with open(BACKUP_FILE, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Created backup: {BACKUP_FILE}")

    # Fix URLs
    fixed_count = 0
    for water_name, water_data in data.items():
        for record in water_data['records']:
            if 'reportUrl' in record and record['reportUrl'].startswith('local://'):
                # Extract filename from local:// URL
                filename = record['reportUrl'].replace('local://', '')
                # Update to /public/reports/ URL
                record['reportUrl'] = f"/public/reports/{filename}"
                fixed_count += 1

    print(f"\nFixed {fixed_count} PDF links")

    # Save updated data
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Saved: {OUTPUT_FILE}")
    print("\n" + "=" * 80)
    print("PDF LINK FIX COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    fix_pdf_links()
