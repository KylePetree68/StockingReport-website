import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os

# The URL of the main stocking report page
BASE_URL = "https://wildlife.dgf.nm.gov"
REPORTS_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/"
# The file where the final JSON data will be saved and read from
OUTPUT_FILE = "stocking_data.json"

def get_pdf_links(page_url):
    """
    Scrapes the main reports page to find links to individual PDF reports
    that explicitly contain "Stocking Report" in the link text.
    """
    print(f"Finding 'Stocking Report' PDF links on {page_url}...")
    pdf_links = []
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        for a_tag in soup.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
            if "?wpdmdl=" in a_tag['href']:
                full_url = a_tag['href']
                if not full_url.startswith('http'):
                    full_url = f"{BASE_URL}{full_url}"
                pdf_links.append(full_url)

        print(f"Found {len(pdf_links)} relevant PDF links.")
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
    Parses the text extracted from the PDF, which has a known, specific format.
    This version includes robust logic to clean hatchery names from water body names.
    """
    all_records = {}
    current_species = None

    # A mapping of hatchery IDs to their full names for cleaner data.
    hatchery_map = {
        'LO': 'Los Ojos Hatchery (Parkview)',
        'PVT': 'Private',
        'RR': 'Red River Trout Hatchery',
        'LS': 'Lisboa Springs Trout Hatchery',
        'RL': 'Rock Lake Trout Rearing Facility'
    }

    # Regex to capture a data line. It's designed to be flexible with spacing.
    # Groups: 1:Water Name/Hatchery, 2:Length, 3:Lbs, 4:Number, 5:Date, 6:Hatchery ID
    data_line_regex = re.compile(
        r"^(.*?)\s+([\d.]+)\s+([\d,.]+)\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z]{2,3})$"
    )

    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if the line is a species header (e.g., "Channel Catfish")
        if re.match(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2}$", line):
            current_species = line
            print(f"  [Parser] Species context set to: {current_species}")
            continue
        
        # Ignore header rows and total rows
        if line.startswith("Water Name") or line.startswith("TOTAL"):
            continue

        match = data_line_regex.match(line)
        if match and current_species:
            name_part, length, _, number, date_str, hatchery_id = match.groups()
            
            water_name = name_part.strip()
            hatchery_name = hatchery_map.get(hatchery_id, hatchery_id)

            # **REVISED CLEANING LOGIC**
            # Use a case-insensitive regex replace to remove the hatchery name from the water name.
            if hatchery_name != 'Private':
                # Escape special characters in hatchery name for regex, like parentheses
                escaped_hatchery_name = re.escape(hatchery_name)
                water_name = re.sub(escaped_hatchery_name, '', water_name, flags=re.IGNORECASE).strip()
            
            # Also handle the 'PRIVATE' case separately and robustly
            water_name = re.sub(r'\s*PRIVATE\s*$', '', water_name, flags=re.IGNORECASE).strip()

            # Final cleanup to remove extra spaces and apply title case
            water_name = " ".join(water_name.split()).title()

            # If after cleaning, the water_name is empty, something went wrong, so skip.
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
                    "hatchery": hatchery_name
                }
                
                print(f"    [+] Found Record for '{water_name}': {record['date']} - {record['species']}")

                if water_name not in all_records:
                    all_records[water_name] = {"reportUrl": report_url, "records": []}
                all_records[water_name]["records"].append(record)

            except ValueError as e:
                print(f"    [!] Skipping record due to error: {e}")
                continue
    
    return all_records


def scrape_reports():
    """
    Main function to orchestrate the scraping process. It loads existing data
    and merges new, unique records into it.
    """
    print("--- Starting Scrape Job ---")
    
    final_data = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing data from {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, "r") as f:
                final_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not read or parse {OUTPUT_FILE}. Starting fresh. Error: {e}")
            final_data = {}
    else:
        print("No existing data file found. Starting fresh.")

    pdf_links = get_pdf_links(REPORTS_PAGE_URL)
    if not pdf_links:
        print("No new PDF links found. Exiting.")
        return

    new_records_found = 0
    for link in pdf_links:
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
        print(f"\nFound a total of {new_records_found} new records.")
        for water_body in final_data:
            final_data[water_body]['records'].sort(key=lambda x: x['date'], reverse=True)
            if pdf_links:
                final_data[water_body]['reportUrl'] = pdf_links[0] 
            
        try:
            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully updated data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("\nNo new records found. Data file may be up-to-date or parser failed to find records.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    # Call the main production function
    scrape_reports()
