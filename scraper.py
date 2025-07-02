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
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def final_parser(text, report_url):
    """
    A final, highly resilient parsing strategy that processes the text line-by-line,
    maintaining the state of the current water body.
    """
    all_records = {}
    current_year = datetime.now().year
    current_water_body = None

    # This regex identifies a line that is *only* a header.
    # It must start with a letter and end with a colon, with no stocking info.
    header_only_pattern = re.compile(r"^([A-Za-z][\w\s.’()\-]+?):$")

    # This regex finds a stocking record and can also capture a header if it's on the same line.
    record_pattern = re.compile(
        r"^(?:([\w\s.’()\-]+?):\s*)?"  # Optional header on the same line
        r"(\w+\s\d+):\s*Stocked\s+([\d,]+)\s+([\w\s\-]+?)\s+\(.*?(\d+\.?\d*)-inch\)"
    )

    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # First, check if the line is ONLY a header. If so, set context and move on.
        header_match = header_only_pattern.match(line)
        if header_match:
            potential_header = header_match.group(1).strip().title()
            # Filter out high-level regional headers
            if "Report" not in potential_header and "Week Of" not in potential_header and "Mexico" not in potential_header and "Northwest" not in potential_header and "Northeast" not in potential_header and "Southwest" not in potential_header and "Southeast" not in potential_header:
                current_water_body = potential_header
                print(f"  [Parser] Context set to: {current_water_body}")
                continue # This line was just a header, so we're done with it.

        # Second, check if the line contains a stocking record.
        record_match = record_pattern.search(line)
        if record_match:
            same_line_header, date_str, quantity, species, length = record_match.groups()
            
            water_body_for_record = None
            if same_line_header:
                water_body_for_record = same_line_header.strip().title()
            elif current_water_body:
                water_body_for_record = current_water_body
            
            if not water_body_for_record:
                continue
            
            try:
                date_obj = datetime.strptime(f"{date_str} {current_year}", "%B %d %Y")
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
                
                print(f"    [+] Found Record for '{water_body_for_record}': {record['date']} - {record['species']}")

                if water_body_for_record not in all_records:
                    all_records[water_body_for_record] = {"reportUrl": report_url, "records": []}
                all_records[water_body_for_record]["records"].append(record)

            except ValueError:
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
                if TEST_DATA_MARKER in content:
                    print("Test data found. Starting with a clean slate.")
                    final_data = {}
                else:
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
            parsed_data = final_parser(raw_text, link)
            
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
    scrape_reports()
