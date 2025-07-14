import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os
import shutil

# The URL of the main stocking report page
BASE_URL = "https://wildlife.dgf.nm.gov"
# We now only need to point to the archive page.
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
# The file where the final JSON data will be saved and read from
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"

def get_pdf_links(page_url):
    """
    Scrapes the archive page to find links to all available PDF reports.
    """
    print(f"Finding all PDF links on the archive page: {page_url}...")
    pdf_links = []
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find all links that seem to be PDF downloads on the archive page
        for a_tag in soup.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
            if "?wpdmdl=" in a_tag['href']:
                full_url = a_tag['href']
                if not full_url.startswith('http'):
                    full_url = f"{BASE_URL}{full_url}"
                pdf_links.append(full_url)

        print(f"Found {len(pdf_links)} total PDF links in the archive.")
        return pdf_links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_url}: {e}")
        return []

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    """
    print(f"  > Extracting text from {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=15)
        response.raise_for_status()
        
        pdf_file = io.BytesIO(response.content)
        
        full_text = ""
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=False)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def parse_stocking_report_text(text, report_url):
    """
    Parses the text extracted from the PDF.
    **CHANGE**: Now adds the reportUrl to each individual record.
    """
    all_records = {}
    current_species = None

    hatchery_map = {
        'LO': 'Los Ojos Hatchery (Parkview)',
        'PVT': 'Private',
        'RR': 'Red River Trout Hatchery',
        'LS': 'Lisboa Springs Trout Hatchery',
        'RL': 'Rock Lake Trout Rearing Facility'
    }

    data_line_regex = re.compile(
        r"^(.*?)\s+([\d.]+)\s+([\d,.]+)\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z]{2,3})$"
    )

    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if re.match(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2}$", line):
            current_species = line
            continue
        
        if line.startswith("Water Name") or line.startswith("TOTAL"):
            continue

        match = data_line_regex.match(line)
        if match and current_species:
            name_part, length, _, number, date_str, hatchery_id = match.groups()
            
            water_name = name_part.strip()
            hatchery_name = hatchery_map.get(hatchery_id, hatchery_id)

            if hatchery_name != 'Private':
                escaped_hatchery_name = re.escape(hatchery_name)
                water_name = re.sub(escaped_hatchery_name, '', water_name, flags=re.IGNORECASE).strip()
            
            water_name = re.sub(r'\s*PRIVATE\s*$', '', water_name, flags=re.IGNORECASE).strip()
            water_name = " ".join(water_name.split()).title()

            if not water_name:
                continue

            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")

                record = {
                    "date": formatted_date,
                    "species": current_species,
                    "quantity": number.replace(',', ''),
                    "length": length,
                    "hatchery": hatchery_name,
                    "reportUrl": report_url # Add the source URL to the record itself
                }
                
                if water_name not in all_records:
                    # The top level no longer needs a reportUrl
                    all_records[water_name] = {"records": []}
                all_records[water_name]["records"].append(record)

            except ValueError as e:
                continue
    
    return all_records


def scrape_reports():
    """
    Main function with corrected efficiency logic.
    """
    print("--- Starting Scrape Job ---")
    
    final_data = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing data from {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, "r") as f:
                final_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not read or parse {OUTPUT_FILE}. Starting fresh.")
            final_data = {}
    else:
        print("No existing data file found. Starting fresh.")

    data_was_modified = False

    # --- Safer One-Time Test Data Cleanup ---
    cleanup_needed = any(
        record.get("date", "").startswith("2024")
        for data in final_data.values()
        for record in data.get("records", [])
    )
    
    if cleanup_needed:
        print("Legacy test data found. Performing one-time cleanup...")
        data_was_modified = True
        cleaned_count = 0
        for water_body in list(final_data.keys()):
            records_to_keep = [
                rec for rec in final_data[water_body].get("records", []) if not rec.get("date", "").startswith("2024")
            ]
            if len(records_to_keep) < len(final_data[water_body].get("records", [])):
                cleaned_count += len(final_data[water_body].get("records", [])) - len(records_to_keep)
                final_data[water_body]["records"] = records_to_keep
        print(f"Removed {cleaned_count} legacy test records.")
    # --- END CLEANUP ---

    # --- **CORRECTED EFFICIENCY LOGIC** ---
    # 1. Get a set of all URLs we have already processed by checking every record.
    processed_urls = set()
    for water_data in final_data.values():
        for record in water_data.get("records", []):
            if "reportUrl" in record:
                processed_urls.add(record["reportUrl"])
    
    # 2. Get all links from the archive page.
    all_pdf_links = get_pdf_links(ARCHIVE_PAGE_URL)
    if not all_pdf_links:
        print("No PDF links found on the archive page. Exiting.")
        if not data_was_modified:
            return

    # 3. Filter for only the new, unprocessed links.
    new_pdf_links = [link for link in all_pdf_links if link not in processed_urls]
    
    print(f"Found {len(processed_urls)} already processed reports.")
    print(f"Found {len(new_pdf_links)} new reports to process.")
    # --- END EFFICIENCY LOGIC ---

    if not new_pdf_links and not data_was_modified:
        print("\nNo new reports to process and no cleanup performed. Data is up-to-date.")
        print("--- Scrape Job Finished ---")
        return

    new_records_found = 0
    for link in new_pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            parsed_data = parse_stocking_report_text(raw_text, link)
            
            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                    new_records_found += len(data['records'])
                else:
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)
                            existing_records_set.add(new_record_str)
                            new_records_found += 1
    
    if new_records_found > 0:
        data_was_modified = True
        print(f"\nFound a total of {new_records_found} new records.")

    if data_was_modified:
        print("Data has been modified. Saving file...")
        for water_body in final_data:
            final_data[water_body]['records'].sort(key=lambda x: x['date'], reverse=True)
        
        try:
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
                print(f"Created backup: {BACKUP_FILE}")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully updated data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("\nNo new records found and no cleanup performed. Data file is up-to-date.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    scrape_reports()
