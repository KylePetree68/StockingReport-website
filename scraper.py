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
                page_text = page.extract_text()
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def parse_pdf_text(text, report_url):
    """
    Parses the raw text extracted from a PDF by reading it line-by-line.
    This is a more robust method than complex regex on the whole text block.
    """
    all_records = {}
    current_year = datetime.now().year
    current_water_body = None

    # Regex to find individual stocking entries.
    stock_pattern = re.compile(
        r"(\w+\s\d+):\s*Stocked\s*([\d,]+)\s*([\w\s\-]+?)\s*\(.*?(\d+\.?\d*)-inch\)"
    )
    
    # Regex to identify a line that is likely a water body header (ends in a colon)
    header_pattern = re.compile(r"^([\w\s.’\-()]+?):$")

    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue

        header_match = header_pattern.match(line)
        # Check if the line is a header (e.g., "Bluewater Lake:")
        if header_match:
            potential_header = header_match.group(1).strip()
            # Filter out non-water-body headers
            if "report" not in potential_header.lower() and "week of" not in potential_header.lower():
                 current_water_body = potential_header.title()
            continue

        # If we have a water body context, look for stocking records
        if current_water_body:
            stock_match = stock_pattern.search(line)
            if stock_match:
                date_str, quantity, species, length = stock_match.groups()
                
                try:
                    # Handle dates that might cross over the new year
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
                    
                    if current_water_body not in all_records:
                        all_records[current_water_body] = {
                            "reportUrl": report_url,
                            "records": []
                        }
                    all_records[current_water_body]["records"].append(record)

                except ValueError:
                    # Silently skip if date is malformed
                    continue
    
    return all_records


def scrape_reports():
    """
    Main function to orchestrate the scraping process. It now loads existing data
    and merges new, unique records into it.
    """
    print("--- Starting Scrape Job ---")
    
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing data from {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, "r") as f:
                final_data = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {OUTPUT_FILE}. Starting fresh.")
            final_data = {}
    else:
        print("No existing data file found. Starting fresh.")
        final_data = {}

    pdf_links = get_pdf_links(REPORTS_PAGE_URL)
    if not pdf_links:
        print("No new PDF links found. Exiting.")
        return

    new_records_found = 0
    for link in pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            parsed_data = parse_pdf_text(raw_text, link)
            
            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                    new_records_found += len(data['records'])
                    print(f"  + Added new water body: {water_body} with {len(data['records'])} records.")
                else:
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)
                            existing_records_set.add(new_record_str)
                            new_records_found += 1
                            print(f"  + Added new record for {water_body} on {new_record['date']}")

    if new_records_found > 0:
        print(f"\nFound a total of {new_records_found} new records.")
        for water_body in final_data:
            final_data[water_body]['records'].sort(key=lambda x: x['date'], reverse=True)
            final_data[water_body]['reportUrl'] = pdf_links[0] 
            
        try:
            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully updated data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("\nNo new records found. Data file is already up-to-date.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    scrape_reports()
