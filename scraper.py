import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os
import shutil
import time

# The URL of the main stocking report page
BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
# The file where the final JSON data will be saved and read from
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"

def get_pdf_links_from_first_page(page_url):
    """
    Scrapes ONLY THE FIRST PAGE of the archive to find the most recent PDF reports.
    """
    print(f"Finding PDF links on the first archive page: {page_url}...")
    pdf_links = []
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        content_div = soup.find("div", class_="post-content")
        if not content_div: return []
        for a_tag in content_div.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
            if "?wpdmdl=" in a_tag['href']:
                full_url = a_tag['href']
                if not full_url.startswith('http'):
                    full_url = f"{BASE_URL}{full_url}"
                pdf_links.append(full_url)
        print(f"Found {len(pdf_links)} PDF links on the first page.")
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
        response = requests.get(pdf_url, timeout=20)
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

def parse_modern_format(text, report_url):
    """
    Parses the modern, table-based PDF format.
    """
    all_records = {}
    current_species = None
    hatchery_map = {'LO': 'Los Ojos Hatchery (Parkview)', 'PVT': 'Private', 'RR': 'Red River Trout Hatchery', 'LS': 'Lisboa Springs Trout Hatchery', 'RL': 'Rock Lake Trout Rearing Facility', 'FED': 'Federal Hatchery'}
    data_line_regex = re.compile(r"^(.*?)\s+([\d.]+)\s+([\d,.]+)\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z]{2,3})$")
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        if re.match(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2}$", line):
            current_species = line.strip()
            continue
        if line.startswith("Water Name") or line.startswith("TOTAL") or line.startswith("Stocking Report By Date"): continue
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
            if not water_name: continue
            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
                record = {"date": formatted_date, "species": current_species, "quantity": number.replace(',', ''), "length": length, "hatchery": hatchery_name, "reportUrl": report_url}
                if water_name not in all_records:
                    all_records[water_name] = {"records": []}
                all_records[water_name]["records"].append(record)
            except ValueError: continue
    return all_records

def parse_older_format(text, report_url):
    """
    Parses older, less-structured PDF formats.
    """
    all_records = {}
    # Find year from report title if available, e.g., "Stocking Report for week of July 4-10, 2020"
    year_match = re.search(r'(\d{4})', text)
    current_year_str = year_match.group(1) if year_match else str(datetime.now().year)
    print(f"    -> Using year {current_year_str} for older format.")

    # Regex to find a water body header and capture all text until the next one.
    water_body_pattern = re.compile(r"([A-Z\s.’()\-]+?):\n(.*?)(?=\n[A-Z\s.’()\-]+?:|\Z)", re.DOTALL)
    stock_pattern = re.compile(r"(\w+\s\d+):\s*Stocked\s*([\d,]+)\s*([\w\s\-]+?)\s*\(.*?(\d+\.?\d*)-inch\)")
    
    for wb_match in water_body_pattern.finditer(text):
        water_body_name = wb_match.group(1).strip().title()
        if "Report" in water_body_name or "Week Of" in water_body_name: continue

        entry_text = wb_match.group(2)
        for stock_match in stock_pattern.finditer(entry_text):
            date_str, quantity, species, length = stock_match.groups()
            try:
                date_obj = datetime.strptime(f"{date_str} {current_year_str}", "%B %d %Y")
                formatted_date = date_obj.strftime("%Y-%m-%d")
                record = {"date": formatted_date, "species": species.strip().title(), "quantity": quantity.replace(',', ''), "length": length, "hatchery": "N/A", "reportUrl": report_url}
                if water_body_name not in all_records:
                    all_records[water_body_name] = {"records": []}
                all_records[water_body_name]["records"].append(record)
            except ValueError: continue
    return all_records

def scrape_reports():
    """
    Main daily function with corrected efficiency logic and fallback parser.
    """
    print("--- Starting Daily Scrape Job ---")
    
    final_data = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing data from {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, "r") as f:
                final_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            print(f"Warning: Could not parse {OUTPUT_FILE}. Starting fresh.")
            final_data = {}
    else:
        print("No existing data file found. Starting fresh.")

    processed_urls = set()
    for water_data in final_data.values():
        for record in water_data.get("records", []):
            if "reportUrl" in record:
                processed_urls.add(record["reportUrl"])
    
    pdf_links_on_first_page = get_pdf_links_from_first_page(ARCHIVE_PAGE_URL)
    if not pdf_links_on_first_page:
        print("No PDF links found on the first archive page. Exiting.")
        return

    new_pdf_links = [link for link in pdf_links_on_first_page if link not in processed_urls]
    
    print(f"Found {len(processed_urls)} already processed reports in total.")
    print(f"Found {len(new_pdf_links)} new reports to process from the first page.")

    if not new_pdf_links:
        print("\nNo new reports to process. Data is up-to-date.")
        print("--- Scrape Job Finished ---")
        return

    new_records_found = 0
    for link in new_pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            # **FIX**: Use the two-stage parsing logic
            parsed_data = parse_modern_format(raw_text, link)
            if not parsed_data:
                print(f"    -> Modern parser failed for {link}. Trying older format parser...")
                parsed_data = parse_older_format(raw_text, link)

            if not parsed_data:
                print(f"    [!] No records found in file with any parser: {link}")
                continue

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
        time.sleep(1) # Be respectful

    if new_records_found > 0:
        print(f"\nFound a total of {new_records_found} new records. Saving file...")
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
        print("\nNo new records were added. File not saved.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    scrape_reports()
