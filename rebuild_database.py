import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os
import shutil

# This script is for a one-time rebuild of the database from the archive.

BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"

def get_pdf_links(page_url):
    print(f"Finding all PDF links on the archive page: {page_url}...")
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
        print(f"Found {len(pdf_links)} total PDF links in the archive.")
        return pdf_links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_url}: {e}")
        return []

def extract_text_from_pdf(pdf_url):
    print(f"  > Processing {pdf_url}...")
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
    all_records = {}
    current_species = None
    hatchery_map = {'LO': 'Los Ojos Hatchery (Parkview)', 'PVT': 'Private', 'RR': 'Red River Trout Hatchery', 'LS': 'Lisboa Springs Trout Hatchery', 'RL': 'Rock Lake Trout Rearing Facility', 'FED': 'Federal Hatchery'}
    data_line_regex = re.compile(r"^(.*?)\s+([\d.]+)\s+([\d,.]+)\s+([\d,]+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z]{2,3})$")
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        if re.match(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2}$", line):
            current_species = line
            continue
        if line.startswith("Water Name") or line.startswith("TOTAL"): continue
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

def rebuild_database():
    print("--- Starting One-Time Database Rebuild ---")
    print("This will process ALL reports from the archive.")
    
    final_data = {}
    all_pdf_links = get_pdf_links(ARCHIVE_PAGE_URL)
    
    if not all_pdf_links:
        print("No PDF links found. Aborting rebuild.")
        return

    for link in all_pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            parsed_data = parse_stocking_report_text(raw_text, link)
            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                else:
                    final_data[water_body]["records"].extend(data["records"])
    
    print(f"\nRebuild complete. Processed {len(all_pdf_links)} reports.")
    
    if final_data:
        print("Saving the newly built database...")
        for water_body in final_data:
            # Remove duplicates and sort
            unique_records = list({json.dumps(rec, sort_keys=True): rec for rec in final_data[water_body]['records']}.values())
            unique_records.sort(key=lambda x: x['date'], reverse=True)
            final_data[water_body]['records'] = unique_records
        
        try:
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
                print(f"Created backup of old file: {BACKUP_FILE}")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully saved new data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("No data was parsed. The data file was not written.")

    print("--- Rebuild Finished ---")

if __name__ == "__main__":
    rebuild_database()
