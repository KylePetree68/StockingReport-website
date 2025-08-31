import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, date
import pdfplumber
import io
import os
import shutil
import time
import sys
import unicodedata

# --- Configuration ---
BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
LIVE_DATA_URL = "https://stockingreport.com/stocking_data.json"
MANUAL_COORDS_FILE = "manual_coordinates.json"

OUTPUT_DIR = "public"
DATA_FILE = os.path.join(OUTPUT_DIR, "stocking_data.json")
BACKUP_FILE = os.path.join(OUTPUT_DIR, "stocking_data.json.bak")
TEMPLATE_FILE = "template.html"
STATIC_PAGES_DIR = os.path.join(OUTPUT_DIR, "waters")
SITEMAP_FILE = os.path.join(OUTPUT_DIR, "sitemap.xml")
BASE_SITE_URL = "https://stockingreport.com"

HATCHERIES = sorted([
    "SEVEN SPRINGS TROUT HATCHERY", "RED RIVER TROUT HATCHERY", 
    "LOS OJOS HATCHERY (PARKVIEW)", "ROCK LAKE TROUT REARING FACILITY",
    "LISBOA SPRINGS TROUT HATCHERY", "FEDERAL HATCHERY", "CLAYTON HATCHERY",
    "PRIVATE" 
], key=len, reverse=True)

SPECIES_LIST = [
    "Brook Trout YY", "Channel Catfish", "Largemouth Bass", "Triploid Rainbow Trout",
    "Gila Trout", "Rio Grande Cutthroat Trout", "Walleye", "Striped Bass", "Wiper", "Kokanee Salmon"
]

# --- Utility Functions ---
def normalize_text(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return ' '.join(text.strip().split())

# --- Scraper Core Logic ---
def get_pdf_links(start_url, all_pages=False, start_year=2025):
    all_pdf_links = set()
    current_page_url = start_url
    page_count = 1
    
    while current_page_url:
        print(f"  Scraping archive page {page_count}: {current_page_url}")
        try:
            response = requests.get(current_page_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            content_div = soup.find("div", id="left-area")
            if not content_div:
                print(f"  [!] Could not find content div on page {page_count}. Stopping.")
                break

            links_on_page = content_div.find_all("a", href=True)
            page_pdf_links = {a['href'] for a in links_on_page if 'stocking-report' in a['href'].lower() and 'wpdmdl' in a['href']}
            
            if not page_pdf_links:
                print(f"  No PDF links found on page {page_count}.")
                break
            
            all_pdf_links.update(page_pdf_links)

            if not all_pages:
                break

            last_link_on_page = sorted(list(page_pdf_links))[-1]
            match = re.search(r'(\d{1,2})[-_](\d{1,2})[-_](\d{2,4})', last_link_on_page)
            if match:
                year_part = match.group(3)
                year = int(f"20{year_part}") if len(year_part) == 2 else int(year_part)
                if year < start_year:
                    print(f"  Reached reports from before {start_year}. Stopping pagination.")
                    break
            
            next_link_tag = soup.find('a', class_='nextpostslink')
            current_page_url = next_link_tag['href'] if next_link_tag else None
            page_count += 1
            time.sleep(0.5)

        except requests.RequestException as e:
            print(f"  [!] Error fetching page {current_page_url}: {e}")
            current_page_url = None

    # Final filter for rebuild to ensure only target year and newer are included
    if all_pages:
        final_links = []
        for link in all_pdf_links:
            match = re.search(r'(\d{1,2})[-_](\d{1,2})[-_](\d{2,4})', link)
            if match:
                year_part = match.group(3)
                year = int(f"20{year_part}") if len(year_part) == 2 else int(year_part)
                if year >= start_year:
                    final_links.append(link)
        print(f"Finished scraping archive. Found {len(final_links)} total PDF links for {start_year} and later.")
        return sorted(final_links, reverse=True)
    else:
        unique_links = sorted(list(all_pdf_links), reverse=True)
        print(f"Finished scraping. Found {len(unique_links)} PDF links on the first page.")
        return unique_links


def parse_pdf_text_line_by_line(pdf_text, pdf_url):
    records = []
    current_species = "Unknown"
    lines = pdf_text.split('\n')
    species_pattern = re.compile(r'^\s*(' + '|'.join(re.escape(s) for s in SPECIES_LIST) + r')\s*$', re.IGNORECASE)
    
    # Pre-compile the record regex for efficiency
    record_regex = re.compile(
        r'^(.*?)'                  # Group 1: Water name and hatchery (non-greedy)
        r'\s+([\d\.]+)'            # Group 2: Length
        r'\s+([\d\.,]+)'           # Group 3: Lbs (we ignore this)
        r'\s+([\d,]+)'             # Group 4: Number/Quantity
        r'\s+(\d{1,2}/\d{1,2}/\d{2,4})' # Group 5: Date
        r'\s+([A-Z]{2,3})\s*$'     # Group 6: Hatchery ID
    )

    for line in lines:
        normalized_line = normalize_text(line)
        if not normalized_line: continue

        species_match = species_pattern.match(normalized_line)
        if species_match:
            matched_species = next((s for s in SPECIES_LIST if s.lower() == species_match.group(1).lower()), "Unknown")
            if matched_species != "Unknown":
                current_species = matched_species
                continue

        match = record_regex.search(normalized_line)
        if match:
            name_part, length, _, quantity, date_str, hatchery_id = match.groups()
            name_part = name_part.strip()
            
            water_name = name_part
            hatchery_name = "Unknown"

            for h in HATCHERIES:
                # Use a regex to find the hatchery name at the end of the string, case-insensitive
                if re.search(r'\s+' + re.escape(h) + '$', name_part, re.IGNORECASE):
                    hatchery_name = h.title().replace("Trout Rearing Facility", "TRF").replace("Hatchery (Parkview)", "Hatchery")
                    # Remove the found hatchery name from the end of the string
                    water_name = re.sub(r'\s+' + re.escape(h) + '$', '', name_part, flags=re.IGNORECASE).strip()
                    break

            try:
                date_format = '%m/%d/%Y' if len(date_str.split('/')[-1]) == 4 else '%m/%d/%y'
                dt_object = datetime.strptime(date_str, date_format)
                formatted_date = dt_object.strftime('%Y-%m-%d')
            except ValueError:
                continue
            
            record = {
                'date': formatted_date, 'species': current_species, 'quantity': quantity.replace(',', ''),
                'length': length, 'hatchery': hatchery_name, 'reportUrl': pdf_url
            }
            records.append((water_name, record))
    return records


def get_coordinates(water_name, session):
    try:
        search_query = f"{water_name}, New Mexico"
        url = f"https://nominatim.openstreetmap.org/search?q={search_query}&format=json&limit=1"
        headers = {'User-Agent': 'NMStockingReport/1.0 (https://stockingreport.com)'}
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            print(f"    [+] Found coordinates for {water_name}: ({lat}, {lon})")
            return {"lat": lat, "lon": lon}
        return None
    except Exception as e:
        print(f"    [!] Error fetching coordinates for {water_name}: {e}")
        return None

def enrich_data_with_coordinates(data):
    print("\n--- Starting Coordinate Enrichment ---")
    manual_coords = {}
    try:
        # Load from the root, as it's part of the repo
        with open(MANUAL_COORDS_FILE, 'r') as f:
            manual_coords = json.load(f)
        print(f"  Loaded {len(manual_coords)} manual coordinate entries.")
    except (FileNotFoundError, json.JSONDecodeError):
        print("  No manual coordinates file found or file is invalid.")

    session = requests.Session()
    waters_to_update = [name for name, details in data.items() if not details.get('coords')]
    
    if not waters_to_update:
        print("All waters already have coordinates. Skipping.")
        return data

    print(f"Found {len(waters_to_update)} waters needing coordinates.")
    for water_name in waters_to_update:
        if water_name in manual_coords and manual_coords[water_name]:
            data[water_name]['coords'] = manual_coords[water_name]
            print(f"    [+] Used manual coordinates for {water_name}")
            continue
        
        coords = get_coordinates(water_name, session)
        if coords:
            data[water_name]['coords'] = coords
        time.sleep(1.1)

    print("--- Finished Coordinate Enrichment ---\n")
    return data

def generate_static_pages_and_sitemap(data):
    print("--- Starting Static Page and Sitemap Generation ---")
    if not os.path.exists(TEMPLATE_FILE):
        print(f"[!] ERROR: Template file '{TEMPLATE_FILE}' not found. Cannot generate pages.")
        return

    with open(TEMPLATE_FILE, 'r') as f:
        template_content = f.read()
    
    sitemap_urls = {BASE_SITE_URL + "/"}
    page_count = 0

    for water_name, water_data in data.items():
        if not water_data.get('records'): continue

        table_rows = ""
        sorted_records = sorted(water_data['records'], key=lambda x: x['date'], reverse=True)
        
        for record in sorted_records:
            dt_utc = datetime.strptime(record['date'], '%Y-%m-%d')
            display_date = f"{dt_utc.month}/{dt_utc.day}/{dt_utc.year}"
            row_onclick = f"window.open('{record['reportUrl']}', '_blank')" if record.get('reportUrl') else ""
            table_rows += f'<tr class="clickable-row" onclick="{row_onclick}">\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{display_date}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800">{record.get("species", "N/A")}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record.get("quantity", "N/A")}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record.get("length", "N/A")}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record.get("hatchery", "N/A")}</td>\n'
            table_rows += '</tr>\n'
        
        filename_safe = re.sub(r'[^a-z0-9]+', '-', water_name.lower()).strip('-')
        filename = f"{filename_safe}.html"
        page_path = os.path.join(STATIC_PAGES_DIR, filename)
        page_url = f"{BASE_SITE_URL}/public/waters/{filename}"
        sitemap_urls.add(page_url)

        page_content = template_content.replace('{{WATER_NAME}}', water_name)
        page_content = page_content.replace('{{PAGE_URL}}', page_url)
        page_content = page_content.replace('{{TABLE_ROWS}}', table_rows)
        
        with open(page_path, 'w') as f:
            f.write(page_content)
        page_count += 1
        
    print(f"  Generated {page_count} static pages.")

    # Generate Sitemap
    xml_content = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url in sorted(list(sitemap_urls)):
        xml_content.append(f'  <url><loc>{url}</loc><lastmod>{date.today().isoformat()}</lastmod></url>')
    xml_content.append('</urlset>')

    try:
        with open(SITEMAP_FILE, 'w') as f:
            f.write('\n'.join(xml_content))
        print(f"  Sitemap generated with {len(sitemap_urls)} URLs.")
    except IOError as e:
        print(f"  [!] Error writing sitemap: {e}")
    print("--- Finished Static Page and Sitemap Generation ---")


def run_scraper(rebuild=False):
    job_type = "One-Time Database Rebuild" if rebuild else "Daily Scrape Job"
    print(f"--- Starting {job_type} ---")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(STATIC_PAGES_DIR, exist_ok=True)
    
    final_data = {}
    
    if rebuild:
        print("REBUILD MODE: Starting fresh, scraping all reports from 2025 onward.")
        pdf_links_to_process = get_pdf_links(ARCHIVE_PAGE_URL, all_pages=True, start_year=2025)
    else: # Daily run
        try:
            print(f"Loading existing data from live site: {LIVE_DATA_URL}")
            response = requests.get(LIVE_DATA_URL, timeout=30)
            response.raise_for_status()
            final_data = response.json()
            print(f"Successfully loaded {len(final_data)} water records from live site.")
        except Exception as e:
            print(f"[!] CRITICAL: Could not load existing data from live URL: {e}.")
            print("[!] ABORTING daily run to prevent data loss.")
            sys.exit(1) # Exit with an error code to fail the build
        
        processed_urls = {
            record.get('reportUrl', '').split('&refresh=')[0]
            for water in final_data.values()
            for record in water.get('records', [])
        }
        
        latest_links = get_pdf_links(ARCHIVE_PAGE_URL, all_pages=False)
        pdf_links_to_process = [link for link in latest_links if link.split('&refresh=')[0] not in processed_urls]

    if not pdf_links_to_process:
        print("\nNo new reports to process. Data file will be re-saved to ensure consistency.")
    else:
        print(f"\nFound {len(pdf_links_to_process)} new reports to process.")
        for pdf_url in pdf_links_to_process:
            print(f"  > Processing: {pdf_url}")
            try:
                pdf_response = requests.get(pdf_url, timeout=30)
                pdf_response.raise_for_status()
                with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                    full_text = "".join(page.extract_text() or "" for page in pdf.pages)
                
                if not full_text.strip(): continue

                parsed_records = parse_pdf_text_line_by_line(full_text, pdf_url)
                
                for water_name, record in parsed_records:
                    if water_name not in final_data:
                        final_data[water_name] = {"records": [], "coords": None}
                    final_data[water_name]["records"].append(record)

            except Exception as e:
                print(f"    [!] Failed to process PDF {pdf_url}: {e}")
            time.sleep(1)

    if not final_data:
        print("[!] No data was loaded or parsed. Aborting to prevent writing an empty file.")
        sys.exit(1)

    # --- Post-processing for BOTH rebuild and daily runs ---
    final_data = enrich_data_with_coordinates(final_data)
    
    print("\nCleaning, de-duplicating, and sorting final data...")
    total_records = 0
    for water_name in list(final_data.keys()):
        water_data = final_data.get(water_name, {})
        if 'records' in water_data and water_data['records']:
            unique_records_map = {}
            for r in water_data['records']:
                key = (r['date'], r['species'], r['quantity'], r['length'], r['hatchery'])
                unique_records_map[key] = r
            
            water_data['records'] = sorted(unique_records_map.values(), key=lambda x: x['date'], reverse=True)
            total_records += len(water_data['records'])
        elif rebuild:
            del final_data[water_name]

    try:
        if os.path.exists(DATA_FILE): shutil.copy(DATA_FILE, BACKUP_FILE)
        with open(DATA_FILE, "w") as f:
            json.dump(final_data, f, indent=4)
        print(f"Successfully saved data file with {len(final_data)} waters and {total_records} total records.")
        
        generate_static_pages_and_sitemap(final_data)

    except IOError as e:
        print(f"[!] FATAL: Error writing to file {DATA_FILE}: {e}")
        sys.exit(1)

    print(f"--- {job_type} Finished ---")

if __name__ == "__main__":
    is_rebuild = "--rebuild" in sys.argv
    run_scraper(rebuild=is_rebuild)

