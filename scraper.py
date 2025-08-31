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
# CORRECTED: Removed /public/ from the live data URL
LIVE_DATA_URL = "https://stockingreport.com/stocking_data.json" 
MANUAL_COORDS_URL = "https://raw.githubusercontent.com/KylePetree68/StockingReport-website/main/manual_coordinates.json"


OUTPUT_DIR = "public"
DATA_FILE = os.path.join(OUTPUT_DIR, "stocking_data.json")
BACKUP_FILE = os.path.join(OUTPUT_DIR, "stocking_data.json.bak")
TEMPLATE_FILE = "template.html"
STATIC_PAGES_DIR = os.path.join(OUTPUT_DIR, "waters")
SITEMAP_FILE = os.path.join(OUTPUT_DIR, "sitemap.xml")
BASE_SITE_URL = "https://stockingreport.com"

HATCHERIES = [
    "SEVEN SPRINGS TROUT HATCHERY", "RED RIVER TROUT HATCHERY", 
    "LOS OJOS HATCHERY (PARKVIEW)", "ROCK LAKE TROUT REARING FACILITY",
    "LISBOA SPRINGS TROUT HATCHERY", "FEDERAL HATCHERY", "CLAYTON HATCHERY",
    "PRIVATE" 
]
# Sort by length descending to match longer names first (e.g., "RED RIVER TROUT HATCHERY" before "PRIVATE")
HATCHERIES.sort(key=len, reverse=True)

# --- Utility Functions ---
def normalize_text(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return ' '.join(text.strip().split())

# --- Scraper Core Logic ---

def get_pdf_links(start_url, all_pages=False, start_year=2025):
    print(f"Finding PDF links. All pages: {all_pages}, Start Year: {start_year}")
    all_pdf_links = []
    current_page_url = start_url
    page_count = 1
    
    while current_page_url:
        print(f"  Scraping archive page {page_count}: {current_page_url}")
        try:
            response = requests.get(current_page_url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            content_div = soup.find("div", class_="et_pb_module et_pb_blog_0_tb_body")
            if not content_div:
                print(f"  [!] Could not find content div on page {page_count}. Stopping.")
                break

            links_on_page = content_div.find_all("a", href=True)
            page_pdf_links = {a['href'] for a in links_on_page if 'stocking-report' in a['href'].lower() and 'wpdmdl' in a['href']}

            if not page_pdf_links:
                print(f"  No PDF links found on page {page_count}.")
                break
            
            all_pdf_links.extend(page_pdf_links)

            if not all_pages:
                break

            # Logic to continue to next page
            next_link_tag = soup.find('a', class_='nextpostslink')
            if next_link_tag and next_link_tag.has_attr('href'):
                 # Check if the next page is still relevant
                last_link_date = '9999'
                last_link_on_page = sorted(list(page_pdf_links))[-1]
                match = re.search(r'(\d{1,2})[-_](\d{1,2})[-_](\d{2,4})', last_link_on_page)
                if match:
                    year_part = match.group(3)
                    year = int(f"20{year_part}") if len(year_part) == 2 else int(year_part)
                    if year < start_year:
                        print(f"  Reached reports from before {start_year}. Stopping pagination.")
                        break # Stop if we've gone past our target year
                
                current_page_url = next_link_tag['href']
                page_count += 1
                time.sleep(0.5)
            else:
                current_page_url = None

        except requests.RequestException as e:
            print(f"  [!] Error fetching page {current_page_url}: {e}")
            current_page_url = None

    unique_links = sorted(list(set(all_pdf_links)), reverse=True)
    print(f"Finished scraping archive. Found {len(unique_links)} total PDF links across {page_count} page(s).")
    return unique_links

def parse_pdf_text(pdf_text, pdf_url):
    records = []
    current_species = "Unknown"
    species_headers = [
        "Brook Trout YY", "Channel Catfish", "Largemouth Bass", "Triploid Rainbow Trout",
        "Gila Trout", "Rio Grande Cutthroat Trout", "Walleye", "Striped Bass", "Wiper", "Kokanee Salmon"
    ]
    
    lines = pdf_text.split('\n')
    
    for line in lines:
        normalized_line = normalize_text(line).strip()
        
        # Check if the line is a species header
        is_header = False
        for header in species_headers:
            if header.lower() == normalized_line.lower():
                current_species = header
                is_header = True
                break
        if is_header:
            continue

        # This regex is designed to be very flexible with spacing
        # It works backwards from the end of the line, which has a more predictable structure.
        match = re.search(
            r'^(.*?)'  # Group 1: Water Name (and possibly hatchery) - Non-greedy
            r'\s+([\d\.]+)'  # Group 2: Length
            r'\s+([\d,\.]+)'  # Group 3: Lbs
            r'\s+([\d,]+)'  # Group 4: Number
            r'\s+(\d{2}/\d{2}/\d{4})'  # Group 5: Date
            r'\s+([A-Z]{2,3})\s*$',  # Group 6: Hatchery ID
            normalized_line
        )

        if match:
            water_name_part, length, _, quantity, date_str, _ = match.groups()
            
            # Now, clean the hatchery from the name part
            water_name = water_name_part
            hatchery_name = "Unknown"

            for h in HATCHERIES:
                if water_name.upper().endswith(h):
                    # Use the found hatchery name, properly capitalized
                    hatchery_name = h.title().replace("Trout Rearing Facility", "TRF").replace("Hatchery (Parkview)", "Hatchery")
                    # Remove it from the water name
                    water_name = water_name[:-len(h)].strip()
                    break

            # Convert date to YYYY-MM-DD format, safely handling timezones
            dt_object = datetime.strptime(date_str, '%m/%d/%Y')
            formatted_date = dt_object.strftime('%Y-%m-%d')

            record = {
                'date': formatted_date,
                'species': current_species,
                'quantity': quantity.replace(',', ''),
                'length': length,
                'hatchery': hatchery_name,
                'reportUrl': pdf_url
            }
            records.append((water_name.strip(), record))
    return records


def get_coordinates(water_name, session):
    """Fetches coordinates for a water body using Nominatim."""
    try:
        # Sanitize name for URL
        search_query = f"{water_name}, New Mexico"
        url = f"https://nominatim.openstreetmap.org/search?q={search_query}&format=json&limit=1"
        headers = {'User-Agent': 'NMStockingReport/1.0 (https://stockingreport.com)'}
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            print(f"    [+] Found coordinates for {water_name}: ({lat}, {lon})")
            return {"lat": lat, "lon": lon}
        else:
            print(f"    [!] No coordinates found for {water_name}")
            return None
    except Exception as e:
        print(f"    [!] Error fetching coordinates for {water_name}: {e}")
        return None

def enrich_data_with_coordinates(data, manual_coords):
    """Adds lat/lon to water bodies in the data file."""
    print("\n--- Starting Coordinate Enrichment ---")
    session = requests.Session()
    waters_to_update = [name for name, details in data.items() if 'coords' not in details or details['coords'] is None]
    
    if not waters_to_update:
        print("All waters already have coordinates. Skipping.")
        return data

    print(f"Found {len(waters_to_update)} waters needing coordinates.")
    
    for water_name in waters_to_update:
        if water_name in manual_coords:
            data[water_name]['coords'] = manual_coords[water_name]
            print(f"    [+] Used manual coordinates for {water_name}")
            continue

        coords = get_coordinates(water_name, session)
        data[water_name]['coords'] = coords
        time.sleep(1.1) # Respect Nominatim's usage policy (1 req/sec)

    print("--- Finished Coordinate Enrichment ---\n")
    return data

def generate_static_pages(data):
    print("\n--- Starting Static Page Generation ---")
    if not os.path.exists(TEMPLATE_FILE):
        print(f"[!] ERROR: Template file '{TEMPLATE_FILE}' not found. Cannot generate pages.")
        return

    with open(TEMPLATE_FILE, 'r') as f:
        template_content = f.read()

    page_count = 0
    for water_name, water_data in data.items():
        if not water_data.get('records'):
            continue

        table_rows = ""
        # Sort records by date descending for display
        sorted_records = sorted(water_data['records'], key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'), reverse=True)
        
        for record in sorted_records:
            # Re-parse date for timezone-safe formatting
            dt_utc = datetime.strptime(record['date'], '%Y-%m-%d')
            display_date = f"{dt_utc.month}/{dt_utc.day}/{dt_utc.year}"
            
            row_onclick = f"window.open('{record['reportUrl']}', '_blank')"
            table_rows += f'<tr class="clickable-row" onclick="{row_onclick}">\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{display_date}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800">{record["species"]}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record["quantity"]}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record["length"]}</td>\n'
            table_rows += f'  <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record["hatchery"]}</td>\n'
            table_rows += '</tr>\n'
        
        filename_safe = water_name.lower().replace('/', ' ').replace('(', '').replace(')', '').replace('&', 'and')
        filename = re.sub(r'[^a-z0-9]+', '-', filename_safe).strip('-') + ".html"
        page_url = f"{BASE_SITE_URL}/public/waters/{filename}" # Correct URL for social tags

        page_content = template_content.replace('{{WATER_NAME}}', water_name)
        page_content = page_content.replace('{{PAGE_URL}}', page_url)
        page_content = page_content.replace('{{TABLE_ROWS}}', table_rows)
        
        filepath = os.path.join(STATIC_PAGES_DIR, filename)
        with open(filepath, 'w') as f:
            f.write(page_content)
        page_count += 1
    print(f"  Generated {page_count} static pages in '{STATIC_PAGES_DIR}'")
    print("--- Finished Static Page Generation ---")


def generate_sitemap(data):
    print("\n--- Starting Sitemap Generation ---")
    urls = {BASE_SITE_URL + "/"}
    for water_name in data:
        if data[water_name].get('records'):
            filename_safe = water_name.lower().replace('/', ' ').replace('(', '').replace(')', '').replace('&', 'and')
            filename = re.sub(r'[^a-z0-9]+', '-', filename_safe).strip('-') + ".html"
            urls.add(f"{BASE_SITE_URL}/public/waters/{filename}")

    xml_content = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url in sorted(list(urls)):
        xml_content.append('  <url>')
        xml_content.append(f'    <loc>{url}</loc>')
        xml_content.append(f'    <lastmod>{date.today().isoformat()}</lastmod>')
        xml_content.append('  </url>')
    xml_content.append('</urlset>')

    try:
        with open(SITEMAP_FILE, 'w') as f:
            f.write('\n'.join(xml_content))
        print(f"  Sitemap generated with {len(urls)} URLs: {SITEMAP_FILE}")
    except IOError as e:
        print(f"  [!] Error writing sitemap: {e}")
    print("--- Finished Sitemap Generation ---")

def run_scraper(rebuild=False):
    job_type = "One-Time Database Rebuild" if rebuild else "Daily Scrape Job"
    print(f"--- Starting {job_type} ---")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(STATIC_PAGES_DIR, exist_ok=True)

    final_data = {}
    
    if rebuild:
        print("REBUILD MODE: Starting with a clean slate and fetching all reports from 2025 onward.")
        pdf_links_to_process = get_pdf_links(ARCHIVE_PAGE_URL, all_pages=True, start_year=2025)
    else:
        try:
            print(f"Loading existing data from live site: {LIVE_DATA_URL}")
            response = requests.get(LIVE_DATA_URL, timeout=20)
            response.raise_for_status()
            final_data = response.json()
            print("Successfully loaded live data.")
        except Exception as e:
            print(f"[!] Could not load existing data from live URL: {e}. This is expected on the first run.")
            final_data = {}
        
        processed_urls = set()
        for water in final_data.values():
            for record in water.get('records', []):
                base_url = record.get('reportUrl', '').split('&refresh=')[0]
                processed_urls.add(base_url)
        
        print("Finding new PDF links on the first page of the archive...")
        latest_links = get_pdf_links(ARCHIVE_PAGE_URL, all_pages=False)
        
        pdf_links_to_process = []
        for link in latest_links:
            base_link = link.split('&refresh=')[0]
            if base_link not in processed_urls:
                pdf_links_to_process.append(link)

    if not pdf_links_to_process:
        print("\nNo new reports to process. Data is up-to-date.")
    else:
        print(f"\nFound {len(pdf_links_to_process)} new reports to process.")
        for pdf_url in pdf_links_to_process:
            print(f"  > Processing: {pdf_url}")
            try:
                pdf_response = requests.get(pdf_url, timeout=30)
                pdf_response.raise_for_status()
                with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                    full_text = "".join(page.extract_text() for page in pdf.pages if page.extract_text())
                
                if not full_text:
                    print(f"    [!] Warning: PDF is empty or text could not be extracted: {pdf_url}")
                    continue

                parsed_records = parse_pdf_text(full_text, pdf_url)
                
                for water_name, record in parsed_records:
                    if water_name not in final_data:
                        final_data[water_name] = {"records": []}
                    final_data[water_name]["records"].append(record)

            except Exception as e:
                print(f"    [!] Failed to process PDF {pdf_url}: {e}")
            time.sleep(1) # Be respectful

    if final_data:
        print("\nEnriching data with coordinates...")
        manual_coords = {}
        try:
            with open(MANUAL_COORDS_FILE, 'r') as f:
                manual_coords = json.load(f)
                print(f"  Loaded {len(manual_coords)} manual coordinate entries.")
        except FileNotFoundError:
            print("  No manual coordinates file found. Will attempt to geocode all missing locations.")
        except json.JSONDecodeError:
            print(f"  [!] Warning: Could not parse {MANUAL_COORDS_FILE}. It might be empty or malformed.")

        final_data = enrich_data_with_coordinates(final_data, manual_coords)

        print("\nCleaning and sorting final data...")
        for water_name, water_data in final_data.items():
            if 'records' in water_data:
                unique_records_map = {}
                for r in water_data['records']:
                    # Create a unique key for each record, ignoring the refresh param
                    key = (r['date'], r['species'], r['quantity'], r['length'], r['hatchery'])
                    unique_records_map[key] = r
                
                water_data['records'] = sorted(unique_records_map.values(), key=lambda x: x['date'], reverse=True)
        
        try:
            if os.path.exists(DATA_FILE):
                shutil.copy(DATA_FILE, BACKUP_FILE)
            with open(DATA_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully saved final data to {DATA_FILE}")

            generate_static_pages(final_data)
            generate_sitemap(final_data)

        except IOError as e:
            print(f"[!] FATAL: Error writing to file {DATA_FILE}: {e}")
    else:
        print("[!] No data available to save. The data file was not written.")

    print(f"--- {job_type} Finished ---")

if __name__ == "__main__":
    is_rebuild = "--rebuild" in sys.argv
    run_scraper(rebuild=is_rebuild)

