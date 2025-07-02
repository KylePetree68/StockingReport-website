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
        
        # Find all links whose text contains "Stocking Report"
        for a_tag in soup.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
            if "?wpdmdl=" in a_tag['href']:
                # Construct the full URL if it's relative
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
    Parses the raw text extracted from a PDF to find stocking records.
    """
    all_records = {}
    current_year = datetime.now().year

    # Regex to find a water body name (often all caps or followed by a colon)
    # and capture all text until the next likely water body name.
    water_body_pattern = re.compile(
        r"([A-Z\s.’\-]+?):?\n(.*?)(?=\n[A-Z\s.’\-]+:?\n|\Z)", 
        re.DOTALL
    )
    # Regex to find individual stocking entries.
    stock_pattern = re.compile(
        r"(\w+\s\d+):\s*Stocked\s*([\d,]+)\s*([\w\s\-]+?)\s*\(.*?(\d+\.?\d*)-inch\)"
    )

    for wb_match in water_body_pattern.finditer(text):
        water_body_name = wb_match.group(1).strip().title()
        if not water_body_name or water_body_name.startswith("Report"): continue
        if water_body_name.endswith(':'): water_body_name = water_body_name[:-1]

        entry_text = wb_match.group(2)
        
        water_records = []
        for stock_match in stock_pattern.finditer(entry_text):
            date_str, quantity, species, length = stock_match.groups()
            
            try:
                # Handle dates that might cross over the new year
                date_obj = datetime.strptime(f"{date_str} {current_year}", "%B %d %Y")
                if date_obj > datetime.now(): # If date is in the future, assume it was last year
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
                water_records.append(record)
            except ValueError:
                # print(f"    Warning: Could not parse date '{date_str}' for {water_body_name}")
                continue
        
        if water_records:
            if water_body_name not in all_records:
                all_records[water_body_name] = {
                    "reportUrl": report_url,
                    "records": []
                }
            all_records[water_body_name]["records"].extend(water_records)

    return all_records

def scrape_reports():
    """
    Main function to orchestrate the scraping process. It now loads existing data
    and merges new, unique records into it.
    """
    print("--- Starting Scrape Job ---")
    
    # --- Step 1: Load existing data from OUTPUT_FILE ---
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing data from {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, "r") as f:
            final_data = json.load(f)
    else:
        print("No existing data file found. Starting fresh.")
        final_data = {}

    # --- Step 2: Scrape the website for new PDF links ---
    pdf_links = get_pdf_links(REPORTS_PAGE_URL)
    if not pdf_links:
        print("No new PDF links found. Exiting.")
        return

    # --- Step 3: Process each PDF and merge new data ---
    new_records_found = 0
    for link in pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            parsed_data = parse_pdf_text(raw_text, link)
            
            # Merge the data from this PDF into our main data structure
            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                    new_records_found += len(data['records'])
                    print(f"  + Added new water body: {water_body} with {len(data['records'])} records.")
                else:
                    # Add only new, unique records
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)
                            existing_records_set.add(new_record_str)
                            new_records_found += 1
                            print(f"  + Added new record for {water_body} on {new_record['date']}")

    # --- Step 4: Clean up and save the final data ---
    if new_records_found > 0:
        print(f"\nFound a total of {new_records_found} new records.")
        # Sort all records by date for each water body to ensure consistency
        for water_body in final_data:
            final_data[water_body]['records'].sort(key=lambda x: x['date'], reverse=True)
            # Update the reportUrl to the most recent one we scraped
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
