#!/usr/bin/env python3
"""
Rebuild the database using locally downloaded PDFs instead of fetching from web.
This is faster and lets us test the improved parser.
"""

import os
import json
import shutil
from datetime import datetime
import pdfplumber
from scraper import final_parser, enrich_data_with_coordinates, generate_static_pages, generate_sitemap

OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"
OUTPUT_DIR = "public/waters"
MANUAL_COORDS_FILE = "manual_coordinates.json"
PDF_DIR = "downloaded_pdfs"

def extract_text_from_local_pdf(pdf_path):
    """Extract text from a local PDF file."""
    try:
        full_text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=False)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_path}: {e}")
        return ""

def main():
    print("=" * 80)
    print("REBUILDING DATABASE FROM LOCAL PDFs")
    print("=" * 80)

    # Create directories if needed
    if not os.path.exists("public"):
        os.makedirs("public")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Load manual coordinates
    manual_coords = {}
    if os.path.exists(MANUAL_COORDS_FILE):
        print(f"\nLoading manual coordinates from {MANUAL_COORDS_FILE}...")
        with open(MANUAL_COORDS_FILE, "r") as f:
            manual_coords = json.load(f)

    # Get all PDFs from downloaded_pdfs directory
    pdf_files = sorted([f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')])
    print(f"\nFound {len(pdf_files)} PDF files in {PDF_DIR}/")

    final_data = {}
    processed = 0
    failed = 0

    print("\n--- Processing PDFs ---\n")

    for i, pdf_file in enumerate(pdf_files, 1):
        pdf_path = os.path.join(PDF_DIR, pdf_file)
        # Create a pseudo-URL for the report
        report_url = f"local://{pdf_file}"

        print(f"[{i}/{len(pdf_files)}] {pdf_file}...")

        # Extract text
        raw_text = extract_text_from_local_pdf(pdf_path)

        if raw_text:
            # Parse the text
            parsed_data = final_parser(raw_text, report_url)

            if not parsed_data:
                print(f"    [!] No records found")
                failed += 1
                continue

            # Merge into final data
            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                else:
                    # Merge records, avoiding duplicates
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)

            processed += 1
            print(f"    [+] Parsed {len(parsed_data)} water bodies")
        else:
            failed += 1

    print(f"\n--- Processing Complete ---")
    print(f"Processed: {processed}/{len(pdf_files)} PDFs")
    print(f"Failed: {failed}")
    print(f"Total unique water bodies: {len(final_data)}")

    if final_data:
        # Enrich with coordinates
        final_data = enrich_data_with_coordinates(final_data, manual_coords)

        # Sort records by date for each water body
        for water_body in final_data:
            unique_records = list({json.dumps(rec, sort_keys=True): rec for rec in final_data[water_body]['records']}.values())
            unique_records.sort(key=lambda x: x['date'], reverse=True)
            final_data[water_body]['records'] = unique_records

        # Backup and save
        try:
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
                print(f"\nCreated backup: {BACKUP_FILE}")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully saved: {OUTPUT_FILE}")

            # Generate static pages and sitemap
            print("\nGenerating static pages and sitemap...")
            generate_static_pages(final_data)
            generate_sitemap(final_data)

            print("\n" + "=" * 80)
            print("REBUILD COMPLETE!")
            print("=" * 80)

        except IOError as e:
            print(f"Error writing file: {e}")
    else:
        print("\nNo data was parsed!")

if __name__ == "__main__":
    main()
