#!/usr/bin/env python3
"""
Weekly incremental update script.

This script:
1. Loads the existing clean stocking data
2. Fetches only NEW reports from the first page of the archive
3. Parses new PDFs and adds records
4. Updates the JSON file, static pages, and sitemap

Run this weekly via GitHub Actions or cron job.
"""

import json
import shutil
import os
import requests
from datetime import datetime
from scraper import (
    get_pdf_links_from_first_page,
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
PDF_DOWNLOAD_DIR = "downloaded_pdfs"

def download_pdf(url, filename):
    """Download a PDF from NMDGF website."""
    os.makedirs(PDF_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs("public/reports", exist_ok=True)

    filepath = os.path.join(PDF_DOWNLOAD_DIR, filename)
    public_filepath = os.path.join("public/reports", filename)

    # Skip if already downloaded
    if os.path.exists(filepath) and os.path.exists(public_filepath):
        return True

    try:
        print(f"  Downloading: {filename}...", end=" ")
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # Save to both locations
            with open(filepath, 'wb') as f:
                f.write(response.content)
            with open(public_filepath, 'wb') as f:
                f.write(response.content)
            print("OK")
            return True
        else:
            print(f"Failed (status {response.status_code})")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def weekly_update():
    """Run weekly incremental update."""
    print("=" * 80)
    print("WEEKLY STOCKING DATA UPDATE")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Create directories if needed
    if not os.path.exists("public"):
        os.makedirs("public")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Load existing clean data
    print(f"\nLoading existing data from {CLEAN_DATA_FILE}...")
    try:
        with open(CLEAN_DATA_FILE, 'r') as f:
            final_data = json.load(f)
        print(f"Loaded {len(final_data)} water bodies")
    except FileNotFoundError:
        print(f"No existing data found, starting fresh")
        final_data = {}

    # Load manual coordinates
    manual_coords = {}
    if os.path.exists(MANUAL_COORDS_FILE):
        print(f"Loading manual coordinates from {MANUAL_COORDS_FILE}...")
        with open(MANUAL_COORDS_FILE, "r") as f:
            manual_coords = json.load(f)

    # Get URLs already processed
    processed_urls = set()
    for water_data in final_data.values():
        for record in water_data.get("records", []):
            if "reportUrl" in record:
                # Normalize URL (remove query params)
                url = record["reportUrl"].split('&refresh=')[0].split('?refresh=')[0]
                processed_urls.add(url)

    print(f"Already processed {len(processed_urls)} unique report URLs")

    # Fetch PDF links from first archive page only
    print(f"\nFetching latest reports from {ARCHIVE_PAGE_URL}...")
    all_pdf_links = get_pdf_links_from_first_page(ARCHIVE_PAGE_URL)

    if not all_pdf_links:
        print("No PDF links found. Aborting.")
        return

    # Filter to only new PDFs
    new_pdf_links = []
    for link in all_pdf_links:
        # Normalize URL for comparison
        normalized = link.split('&refresh=')[0].split('?refresh=')[0]
        if normalized not in processed_urls:
            new_pdf_links.append(link)

    if not new_pdf_links:
        print("\nNo new reports to process. Data is already up-to-date!")
        # Still regenerate pages/sitemap in case template changed
        print("\nRegenerating static pages and sitemap...")
        generate_static_pages(final_data)
        generate_sitemap(final_data)
        print("\n" + "=" * 80)
        print("UPDATE COMPLETE - No changes")
        print("=" * 80)
        return

    print(f"\nFound {len(new_pdf_links)} NEW reports to process:")
    for i, link in enumerate(new_pdf_links, 1):
        print(f"  {i}. {link}")

    # Download new PDFs
    print(f"\n--- Downloading {len(new_pdf_links)} PDFs ---\n")
    for i, pdf_url in enumerate(new_pdf_links, 1):
        try:
            # Extract filename from URL
            # Example: /download/stocking-report-8-29-25/?wpdmdl=...
            parts = pdf_url.split('/download/')[1].split('?')[0].strip('/')
            filename = parts + '.pdf'
            download_pdf(pdf_url, filename)
        except Exception as e:
            print(f"  [!] Could not download {pdf_url}: {e}")

    # Process new PDFs
    print("\n--- Processing New Reports ---\n")
    new_records_count = 0

    for i, link in enumerate(new_pdf_links, 1):
        print(f"[{i}/{len(new_pdf_links)}] Processing {link}...")

        raw_text = extract_text_from_pdf(link)
        if not raw_text:
            print(f"  [!] Failed to extract text")
            continue

        parsed_data = final_parser(raw_text, link)
        if not parsed_data:
            print(f"  [!] No records found")
            continue

        # Merge into final data
        for water_body, data in parsed_data.items():
            if water_body not in final_data:
                final_data[water_body] = data
                new_records_count += len(data['records'])
                print(f"  [+] New water body: {water_body} ({len(data['records'])} records)")
            else:
                # Add new records, avoiding duplicates
                existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                added = 0
                for new_record in data['records']:
                    new_record_str = json.dumps(new_record, sort_keys=True)
                    if new_record_str not in existing_records_set:
                        final_data[water_body]['records'].append(new_record)
                        added += 1
                        new_records_count += 1

                if added > 0:
                    print(f"  [+] {water_body}: added {added} new records")

    print(f"\n--- Processing Complete ---")
    print(f"New records added: {new_records_count}")
    print(f"Total water bodies: {len(final_data)}")

    if new_records_count > 0:
        # Enrich any new water bodies with coordinates
        final_data = enrich_data_with_coordinates(final_data, manual_coords)

        # Sort records by date for each water body
        for water_body in final_data:
            unique_records = list({json.dumps(rec, sort_keys=True): rec for rec in final_data[water_body]['records']}.values())
            unique_records.sort(key=lambda x: x['date'], reverse=True)
            final_data[water_body]['records'] = unique_records

        # Backup and save
        print(f"\nSaving updated data...")
        try:
            # Backup old clean file
            if os.path.exists(CLEAN_DATA_FILE):
                backup = CLEAN_DATA_FILE.replace('.json', '_backup.json')
                shutil.copy(CLEAN_DATA_FILE, backup)
                print(f"Created backup: {backup}")

            # Save clean data
            with open(CLEAN_DATA_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Saved: {CLEAN_DATA_FILE}")

            # Also update the regular output file for compatibility
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Saved: {OUTPUT_FILE}")

            # Generate static pages and sitemap
            print("\nGenerating static pages and sitemap...")
            generate_static_pages(final_data)
            generate_sitemap(final_data)

            print("\n" + "=" * 80)
            print("UPDATE COMPLETE!")
            print("=" * 80)
            print(f"Added {new_records_count} new stocking records")
            print(f"Total water bodies: {len(final_data)}")

        except IOError as e:
            print(f"Error writing files: {e}")
    else:
        print("\nNo new records were added.")

if __name__ == "__main__":
    weekly_update()
