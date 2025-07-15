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

# This script is for a one-time, full rebuild of the database from all archive pages.

BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"

def get_all_pdf_links_from_archive(start_url):
    """
    Scrapes ALL pages of the archive to find links to all available PDF reports.
    """
    print(f"Finding all PDF links, starting from: {start_url}...")
    all_pdf_links = []
    current_page_url = start_url
    page_count = 1

    while current_page_url:
        print(f"  Scraping archive page {page_count}: {current_page_url}")
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            content_div = soup.find("div", class_="post-content")
            if not content_div:
                print(f"    Could not find content div on page {page_count}. Stopping.")
                break

            for a_tag in content_div.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
                if "?wpdmdl=" in a_tag['href']:
                    full_url = a_tag['href']
                    if not full_url.startswith('http'):
                        full_url = f"{BASE_URL}{full_url}"
                    if full_url not in all_pdf_links:
                        all_pdf_links.append(full_url)
            
            next_link = soup.find("a", class_="next")
            if next_link and next_link.has_attr('href'):
                current_page_url = next_link['href']
                page_count += 1
                time.sleep(1)
            else:
                current_page_url = None

            if page_count > 25:
                print("    Reached page limit of 25. Stopping.")
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {current_page_url}: {e}")
            break

    print(f"\nFinished scraping archive. Found {len(all_pdf_links)} total PDF links across {page_count} pages.")
    return all_pdf_links

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    """
    print(f"  > Processing {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=30)
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

def final_parser(text, report_url):
    """
    A robust parser that splits lines into words and works backwards to avoid errors.
    """
    all_records = {}
    current_species = None
    hatchery_map = {
        'LO': 'Los Ojos Hatchery (Parkview)',
        'PVT': 'Private',
        'RR': 'Red River Trout Hatchery',
        'LS': 'Lisboa Springs Trout Hatchery',
        'RL': 'Rock Lake Trout Rearing Facility',
        'FED': 'Federal Hatchery',
        'SS': 'Seven Springs Trout Hatchery'
    }
    
    species_regex = re.compile(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+)*$")
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if species_regex.match(line) and "By Date For" not in line:
            current_species = line.strip()
            continue
            
        if line.startswith("Water Name") or line.startswith("TOTAL") or line.startswith("Stocking Report By Date"): continue
        
        words = line.split()
        # A valid data line must have at least 6 columns (name can be multi-word)
        if len(words) < 6: continue

        try:
            # Work backwards from the end of the line, which is more predictable
            hatchery_id = words[-1]
            date_str = words[-2]
            number = words[-3]
            # lbs = words[-4] # We don't use this column
            length = words[-5]
            
            # Everything else is part of the name
            name_part = " ".join(words[:-5])

            # Validate that the extracted parts look correct
            if not re.match(r"\d{2}\/\d{2}\/\d{4}", date_str): continue
            if not hatchery_id in hatchery_map: continue

            hatchery_name = hatchery_map.get(hatchery_id)
            water_name = " ".join(name_part.split()).title()
            
            if not water_name: continue
            
            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            formatted_date = date_obj.strftime("%Y-%m-%d")
            
            record = {"date": formatted_date, "species": current_species, "quantity": number.replace(',', ''), "length": length, "hatchery": hatchery_name, "reportUrl": report_url}
            
            if water_name not in all_records:
                all_records[water_name] = {"records": []}
            all_records[water_name]["records"].append(record)

        except (ValueError, IndexError):
            # This line didn't match the expected format, so we skip it
            continue
            
    return all_records

def rebuild_database():
    """
    This function performs a one-time, full rebuild of the database.
    """
    print("--- Starting One-Time Database Rebuild ---")
    print("This will process ALL reports from the archive.")
    
    final_data = {}
    all_pdf_links = get_all_pdf_links_from_archive(ARCHIVE_PAGE_URL)
    
    if not all_pdf_links:
        print("No PDF links found. Aborting rebuild.")
        return

    for link in all_pdf_links:
        raw_text = extract_text_from_pdf(link)
        if raw_text:
            parsed_data = final_parser(raw_text, link)
            if not parsed_data:
                print(f"    [!] No records found in file: {link}")
                continue

            for water_body, data in parsed_data.items():
                if water_body not in final_data:
                    final_data[water_body] = data
                else:
                    final_data[water_body]["records"].extend(data["records"])
        time.sleep(1)
    
    print(f"\nRebuild complete. Processed {len(all_pdf_links)} reports.")
    
    if final_data:
        print("Saving the newly built database...")
        for water_body in final_data:
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
