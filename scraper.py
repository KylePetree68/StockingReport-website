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
import sys

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
    A robust parser that uses a definitive list of hatchery names to correctly
    separate water names from the rest of the data.
    """
    all_records = {}
    current_species = None
    
    hatchery_map = {
        'LO': 'Los Ojos Hatchery (Parkview)', 'PVT': 'Private', 'RR': 'Red River Trout Hatchery',
        'LS': 'Lisboa Springs Trout Hatchery', 'RL': 'Rock Lake Trout Rearing Facility',
        'FED': 'Federal Hatchery', 'SS': 'Seven Springs Trout Hatchery', 'GW': 'Glenwood Springs Hatchery'
    }
    hatchery_names_sorted = sorted(hatchery_map.values(), key=len, reverse=True)
    
    species_regex = re.compile(r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+)*$")
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if species_regex.match(line) and "By Date For" not in line:
            current_species = line.strip()
            continue
            
        if line.startswith("Water Name") or line.startswith("TOTAL") or line.startswith("Stocking Report By Date"): continue
        
        # **FINAL, ROBUST FIX**: Use hatchery names as delimiters.
        for h_name in hatchery_names_sorted:
            # Use a case-insensitive regex to find the hatchery name
            match = re.search(r'\b' + re.escape(h_name) + r'\b', line, re.IGNORECASE)
            if match:
                name_part = line[:match.start()].strip()
                data_part = line[match.end():].strip()
                
                # Now parse the remaining data part
                data_words = data_part.split()
                if len(data_words) < 5: continue

                try:
                    hatchery_id = data_words[-1]
                    date_str = data_words[-2]
                    number = data_words[-3]
                    length = data_words[-5]

                    if not re.match(r"\d{2}\/\d{2}\/\d{4}", date_str): continue
                    if hatchery_id not in hatchery_map: continue

                    water_name = " ".join(name_part.split()).title()
                    if not water_name: continue

                    date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                    
                    record = {"date": formatted_date, "species": current_species, "quantity": number.replace(',', ''), "length": length, "hatchery": hatchery_map.get(hatchery_id), "reportUrl": report_url}
                    
                    if water_name not in all_records:
                        all_records[water_name] = {"records": []}
                    all_records[water_name]["records"].append(record)
                    break # Move to the next line once we've found a match

                except (ValueError, IndexError):
                    continue
            
    return all_records

def run_scraper(rebuild=False):
    """
    Main function to orchestrate the scraping process.
    """
    if rebuild:
        print("--- Starting One-Time Database Rebuild ---")
        final_data = {}
        all_pdf_links = get_all_pdf_links_from_archive(ARCHIVE_PAGE_URL)
        if not all_pdf_links:
            print("No PDF links found. Aborting rebuild.")
            return
    else:
        print("--- Starting Daily Scrape Job ---")
        final_data = {}
        if os.path.exists(OUTPUT_FILE):
            print(f"Loading existing data from {OUTPUT_FILE}...")
            try:
                with open(OUTPUT_FILE, "r") as f:
                    final_data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not parse {OUTPUT_FILE}. Error: {e}. Aborting to prevent data loss.")
                return
        
        processed_urls = set()
        for water_data in final_data.values():
            for record in water_data.get("records", []):
                if "reportUrl" in record:
                    processed_urls.add(record["reportUrl"])
        
        all_pdf_links = get_pdf_links_from_first_page(ARCHIVE_PAGE_URL)
        new_pdf_links = [link for link in all_pdf_links if link not in processed_urls]
        
        if not new_pdf_links:
            print("\nNo new reports to process. Data is up-to-date.")
            print("--- Scrape Job Finished ---")
            return
        
        print(f"Found {len(new_pdf_links)} new reports to process.")
        all_pdf_links = new_pdf_links

    # Process the selected links (either all for rebuild, or new for daily)
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
                    # Add only new records
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)
        time.sleep(1)
    
    print("\nScrape complete. Saving data...")
    
    if final_data:
        for water_body in final_data:
            unique_records = list({json.dumps(rec, sort_keys=True): rec for rec in final_data[water_body]['records']}.values())
            unique_records.sort(key=lambda x: x['date'], reverse=True)
            final_data[water_body]['records'] = unique_records
        
        try:
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
                print(f"Created backup: {BACKUP_FILE}")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully saved new data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("No data was parsed. The data file was not written.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    # Check for a command-line argument to trigger a rebuild
    if "--rebuild" in sys.argv:
        run_scraper(rebuild=True)
    else:
        run_scraper(rebuild=False)
