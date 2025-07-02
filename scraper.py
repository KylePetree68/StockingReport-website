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
# A simple way to identify if the file still contains our original test data
TEST_DATA_MARKER = "Tingley Beach" 

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
                # Using a layout-aware extraction can sometimes help with tables/columns
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def parse_pdf_text_stateful(text, report_url):
    """
    A more robust, stateful parser that reads line-by-line and maintains context.
    This is designed to be highly resilient to PDF formatting quirks.
    """
    all_records = {}
    current_year = datetime.now().year
    current_water_body = None

    # Regex to find a line that is likely a water body header.
    # It looks for capitalized words, possibly with '()', ending in a colon.
    header_pattern = re.compile(r"^([A-Z][\w\s.’()\-]+?):$")

    # Regex to find the key components of a stocking record.
    date_pattern = re.compile(r"(\w+\s\d+)")
    stock_details_pattern = re.compile(r"Stocked\s+([\d,]+)\s+([\w\s\-]+?)\s+\(.*?(\d+\.?\d*)-inch\)")

    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Is this line a header? Check and set context if it is.
        header_match = header_pattern.match(line)
        if header_match:
            potential_header = header_match.group(1).strip().title()
            if "report" not in potential_header.lower() and "week of" not in potential_header.lower():
                current_water_body = potential_header
                print(f"  [Parser] Context set to: {current_water_body}")
        
        # ** CRITICAL FIX **
        # After checking for a header, ALWAYS check for a stocking record on the SAME line.
        # This handles cases where the header and record are on the same line.
        if current_water_body:
            date_match = date_pattern.search(line)
            details_match = stock_details_pattern.search(line)

            if date_match and details_match:
                date_str = date_match.group(1)
                quantity, species, length = details_match.groups()

                try:
                    date_obj = datetime.strptime(f"{date_str} {current_year}", "%B %d %Y")
                    # If the parsed date is in the future, assume it was from the previous year.
                    if date_obj > datetime.now():
                        date_obj = date_obj.replace(year=current_year - 1)
                    
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                    clean_quantity = quantity.replace(',', '')
                    clean_species = " ".join(species.strip().title().split())

                    record = {
                        "date": formatted_date,
                        "species": clean_species,
                        "quantity": clean_quantity,
                        "length": length,
                        "hatchery": "N/A"
                    }
                    
                    print(f"  [Parser] Found Record: {current_water_body} -> {record['date']} - {record['species']}")

                    if current_water_body not in all_records:
                        all_records[current_water_body] = {"reportUrl": report_url, "records": []}
                    all_records[current_water_body]["records"].append(record)

                except ValueError:
                    # Date parsing failed, likely not a real record.
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
                content = f.read()
                # Check if the file contains our test data. If so, start fresh.
                if TEST_DATA_MARKER in content:
                    print("Test data found. Starting with a clean slate.")
                    final_data = {}
                else:
                    # Rewind and parse the JSON
                    final_data = json.loads(content)
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
            parsed_data = parse_pdf_text_stateful(raw_text, link)
            
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
            # Ensure the reportUrl is the most recent one scraped
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
    scrape_reports()
