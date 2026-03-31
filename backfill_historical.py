#!/usr/bin/env python3
"""
Backfill historical stocking data from the NMDGF archive.

This script:
1. Crawls ALL pages of the NMDGF archive to find every report URL
2. Compares against already-processed URLs in stocking_data_clean.json
3. Downloads and parses any missing reports
4. Merges new records into the existing data, skipping malformed ones

Run this manually when historical data gaps are detected.

Known parser limitation (older 2022 PDFs):
  When a large stocking's weight column wraps to the next line alongside
  "(PARKVIEW)", the parser misreads the length field as "HATCHERY" and
  leaves "Los Ojos" in the water name. These records are filtered out below.
  If a full re-scrape is ever needed, the parser should be fixed to handle
  this wrapping case before running.
"""

import json
import re
import shutil
import os
import requests
from datetime import datetime
from scraper import (
    get_pdf_links_for_rebuild,
    extract_text_from_pdf,
    final_parser,
    enrich_data_with_coordinates,
    generate_static_pages,
    generate_sitemap,
    ARCHIVE_PAGE_URL,
    OUTPUT_FILE,
    BACKUP_FILE,
    OUTPUT_DIR,
    MANUAL_COORDS_FILE
)

CLEAN_DATA_FILE = "stocking_data_clean.json"
REPORTS_DIR = "public/reports"


def is_valid_length(length_str):
    """Return True if length looks like a real fish measurement (numeric or range like 8-10)."""
    if not length_str:
        return False
    s = str(length_str).strip()
    # Allow plain numbers (e.g. "9.3", "10") and ranges (e.g. "8-10")
    if re.match(r'^\d+(\.\d+)?$', s):
        return True
    if re.match(r'^\d+(\.\d+)?-\d+(\.\d+)?$', s):
        return True
    return False


def download_pdf(url, filename):
    """Download a PDF to public/reports/. Skip if already cached."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filepath = os.path.join(REPORTS_DIR, filename)

    if os.path.exists(filepath):
        print(f"  Already cached: {filename}")
        return True

    try:
        print(f"  Downloading: {filename}...", end=" ", flush=True)
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print("OK")
            return True
        else:
            print(f"Failed (HTTP {response.status_code})")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def backfill():
    print("=" * 80)
    print("HISTORICAL BACKFILL")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    os.makedirs("public", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load existing clean data
    print(f"\nLoading existing data from {CLEAN_DATA_FILE}...")
    try:
        with open(CLEAN_DATA_FILE, 'r') as f:
            final_data = json.load(f)
        total_existing = sum(len(v['records']) for v in final_data.values())
        print(f"Loaded {len(final_data)} water bodies, {total_existing} total records")
    except FileNotFoundError:
        print("No existing data found, starting fresh")
        final_data = {}

    # Load manual coordinates
    manual_coords = {}
    if os.path.exists(MANUAL_COORDS_FILE):
        with open(MANUAL_COORDS_FILE, "r") as f:
            manual_coords = json.load(f)

    # Collect already-processed URLs (normalize to strip refresh tokens)
    processed_urls = set()
    for water_data in final_data.values():
        for record in water_data.get("records", []):
            if "reportUrl" in record:
                url = record["reportUrl"].split('&refresh=')[0].split('?refresh=')[0]
                processed_urls.add(url)
    print(f"Already processed {len(processed_urls)} unique report URLs")

    # Crawl the full archive
    print(f"\nCrawling full NMDGF archive at {ARCHIVE_PAGE_URL}...")
    all_pdf_links = get_pdf_links_for_rebuild(ARCHIVE_PAGE_URL)
    print(f"Found {len(all_pdf_links)} total report URLs in archive")

    # Find unprocessed URLs
    new_pdf_links = []
    for link in all_pdf_links:
        normalized = link.split('&refresh=')[0].split('?refresh=')[0]
        if normalized not in processed_urls:
            new_pdf_links.append(link)

    if not new_pdf_links:
        print("\nNo missing reports found - data is already complete!")
        return

    print(f"\nFound {len(new_pdf_links)} reports not yet in the database:")
    for i, link in enumerate(new_pdf_links, 1):
        print(f"  {i}. {link}")

    # Download missing PDFs to public/reports/
    print(f"\n--- Downloading {len(new_pdf_links)} PDFs ---\n")
    download_failures = []
    for pdf_url in new_pdf_links:
        try:
            parts = pdf_url.split('/download/')[1].split('?')[0].strip('/')
            filename = parts + '.pdf'
            if not download_pdf(pdf_url, filename):
                download_failures.append(pdf_url)
        except Exception as e:
            print(f"  [!] Could not process URL {pdf_url}: {e}")
            download_failures.append(pdf_url)

    if download_failures:
        print(f"\nWarning: {len(download_failures)} PDFs failed to download:")
        for url in download_failures:
            print(f"  {url}")

    # Parse and merge missing PDFs
    print("\n--- Processing Missing Reports ---\n")
    new_records_count = 0
    skipped_malformed = 0
    parse_failures = []

    for i, link in enumerate(new_pdf_links, 1):
        print(f"[{i}/{len(new_pdf_links)}] {link}")

        raw_text = extract_text_from_pdf(link)
        if not raw_text:
            print(f"  [!] Failed to extract text - skipping")
            parse_failures.append(link)
            continue

        parsed_data = final_parser(raw_text, link)
        if not parsed_data:
            print(f"  [!] No records parsed - PDF format may be unrecognized")
            parse_failures.append(link)
            continue

        report_records = sum(len(v['records']) for v in parsed_data.values())
        print(f"  Parsed {len(parsed_data)} water bodies, {report_records} records")

        for water_body, data in parsed_data.items():
            valid_records = []
            for rec in data['records']:
                if not is_valid_length(rec.get('length')):
                    print(f"  [skip] Malformed record in '{water_body}': length='{rec.get('length')}' date={rec.get('date')}")
                    skipped_malformed += 1
                    continue
                valid_records.append(rec)

            if not valid_records:
                continue

            if water_body not in final_data:
                final_data[water_body] = {"records": valid_records}
                new_records_count += len(valid_records)
            else:
                existing_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                added = 0
                for new_record in valid_records:
                    if json.dumps(new_record, sort_keys=True) not in existing_set:
                        final_data[water_body]['records'].append(new_record)
                        added += 1
                        new_records_count += 1

    print(f"\n--- Processing Complete ---")
    print(f"New records added:      {new_records_count}")
    print(f"Malformed records skipped: {skipped_malformed}")
    print(f"Total water bodies:     {len(final_data)}")

    if parse_failures:
        print(f"\nWarning: {len(parse_failures)} PDFs could not be parsed:")
        for url in parse_failures:
            print(f"  {url}")

    if new_records_count == 0:
        print("\nNo new records were added.")
        return

    # Enrich coordinates for any new water bodies
    final_data = enrich_data_with_coordinates(final_data, manual_coords)

    # Sort records by date for each water body
    for water_body in final_data:
        unique_records = list({json.dumps(rec, sort_keys=True): rec for rec in final_data[water_body]['records']}.values())
        unique_records.sort(key=lambda x: x['date'], reverse=True)
        final_data[water_body]['records'] = unique_records

    # Backup and save
    print(f"\nSaving updated data...")
    if os.path.exists(CLEAN_DATA_FILE):
        backup = CLEAN_DATA_FILE.replace('.json', '_backup.json')
        shutil.copy(CLEAN_DATA_FILE, backup)
        print(f"Backup saved: {backup}")

    with open(CLEAN_DATA_FILE, "w") as f:
        json.dump(final_data, f, indent=4)
    print(f"Saved: {CLEAN_DATA_FILE}")

    if os.path.exists(OUTPUT_FILE):
        shutil.copy(OUTPUT_FILE, BACKUP_FILE)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_data, f, indent=4)
    print(f"Saved: {OUTPUT_FILE}")

    # Regenerate static pages and sitemap
    print("\nRegenerating static pages and sitemap...")
    generate_static_pages(final_data)
    generate_sitemap(final_data)

    total_after = sum(len(v['records']) for v in final_data.values())
    print("\n" + "=" * 80)
    print("BACKFILL COMPLETE!")
    print(f"Added {new_records_count} historical stocking records")
    print(f"Total: {len(final_data)} water bodies, {total_after} records")
    print("=" * 80)
    print("\nNext steps:")
    print("  git add stocking_data_clean.json stocking_data.json public/")
    print("  git commit -m 'Backfill historical stocking data'")
    print("  git push")


if __name__ == "__main__":
    backfill()
