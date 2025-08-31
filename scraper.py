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
import unicodedata

# --- Configuration ---
ARCHIVE_PAGE_URL = "https://wildlife.dgf.nm.gov/fishing/weekly-report/fish-stocking-archive/"
LIVE_DATA_URL = "https://stockingreport.com/public/stocking_data.json"
OUTPUT_FILE = "public/stocking_data.json"
BACKUP_FILE = "public/stocking_data.json.bak"
TEMPLATE_FILE = "template.html"
STATIC_PAGES_DIR = "public/waters"
SITEMAP_FILE = "public/sitemap.xml"
MANUAL_COORDS_FILE = "manual_coordinates.json"
BASE_SITE_URL = "https://stockingreport.com"

# Definitive list of known hatcheries for cleaning names
HATCHERIES = [
    "SEVEN SPRINGS TROUT HATCHERY", "RED RIVER TROUT HATCHERY", "PRIVATE",
    "LOS OJOS HATCHERY (PARKVIEW)", "ROCK LAKE TROUT REARING FACILITY",
    "LISBOA SPRINGS TROUT HATCHERY", "FEDERAL HATCHERY", "CLAYTON HATCHERY"
]
HATCHERIES.sort(key=len, reverse=True) # Sort by length to match longest first

# --- Utility Functions ---

def normalize_text(text):
    """Normalizes text by removing extra spaces and special characters."""
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return ' '.join(text.strip().split())

def get_pdf_links_from_url(page_url):
    """Gets all PDF links from a single archive page."""
    try:
        response = requests.get(page_url, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        content_div = soup.find("div", class_="et_pb_module et_pb_blog_0_tb_body")
        if not content_div:
            print(f"  [!] Could not find content div on page: {page_url}. Skipping.")
            return []
        
        links = content_div.find_all("a", href=True)
        pdf_links = [a['href'] for a in links if 'stocking-report' in a['href'].lower() and a['href'].endswith('.pdf')]
        # Fallback for links that don't end in .pdf but have the download attribute
        download_links = [a['href'] for a in links if 'wpdmdl' in a.get('href', '')]
        
        # Combine and remove duplicates
        all_links = sorted(list(set(pdf_links + download_links)))
        return all_links
    except requests.RequestException as e:
        print(f"  [!] Error fetching page {page_url}: {e}")
        return []

def get_pdf_links_for_rebuild(start_url):
    """Scrapes all archive pages starting from 2025."""
    target_year = 2025
    print(f"--- Finding all PDF links for year {target_year} and later ---")
    all_pdf_links = []
    current_page_url = start_url
    page_count = 1
    
    while current_page_url:
        print(f"  Scraping archive page {page_count}: {current_page_url}")
        page_links = get_pdf_links_from_url(current_page_url)
        
        if not page_links:
            print(f"  No links found on page {page_count}. Stopping.")
            break

        # Check if the page contains reports from before the target year
        reports_before_target = any(f"-{target_year-1}" in link or f"_{target_year-1}" in link for link in page_links)
        
        for link in page_links:
             # Extract year from URL to decide if we should keep it
            match = re.search(r'(\d{1,2})[-_](\d{1,2})[-_](\d{2,4})', link)
            if match:
                year_part = match.group(3)
                year = int(f"20{year_part}") if len(year_part) == 2 else int(year_part)
                if year >= target_year:
                    all_pdf_links.append(link)

        if reports_before_target:
            print(f"  Found reports from before {target_year}. Stopping pagination.")
            break

        # Find the "Next" page link
        try:
            response = requests.get(current_page_url, timeout=20)
            soup = BeautifulSoup(response.content, 'html.parser')
            next_link = soup.find('a', class_='nextpostslink')
            current_page_url = next_link['href'] if next_link else None
            page_count += 1
            time.sleep(0.5) # Be respectful
        except requests.RequestException as e:
            print(f"  [!] Error fetching next page link from {current_page_url}: {e}")
            current_page_url = None

    print(f"Finished scraping archive. Found {len(all_pdf_links)} total PDF links for {target_year} and later.")
    return list(set(all_pdf_links))


def parse_pdf_text(pdf_text, pdf_url):
    """Parses text extracted from a PDF to find stocking records."""
    records = []
    current_species = "Unknown"
    
    # Regex to find a species header (multi-word, title case, ends with a noun)
    species_regex = re.compile(r'^\s*(([A-Z][a-z]+(?:\s|$))+)\s*$')
    # Regex to capture a data line. More robust to handle variations.
    record_regex = re.compile(
        r'(.+?)\s{2,}'  # Water Name (non-greedy) followed by 2+ spaces
        r'([\d\.,]+)\s+' # Length
        r'([\d\.,]+)\s+' # Lbs
        r'([\d\.,]+)\s+' # Number
        r'(\d{2}/\d{2}/\d{4})\s+' # Date MM/DD/YYYY
        r'([A-Z]{2,3})\s*$' # Hatchery ID
    )

    lines = pdf_text.split('\n')
    for line in lines:
        line = normalize_text(line)
        if not line:
            continue

        species_match = species_regex.match(line)
        if species_match and "Total" not in line and "Page" not in line:
            potential_species = species_match.group(1).strip()
            # A simple filter to avoid capturing table headers as species
            if "hatchery" not in potential_species.lower() and "water name" not in potential_species.lower():
                 current_species = potential_species
                 #print(f"  [Parser] Species context set to: {current_species}")
                 continue
        
        record_match = record_regex.search(line)
        if record_match:
            full_name_part = record_match.group(1).strip()
            
            # Clean hatchery name from the water body name
            water_name = full_name_part
            hatchery_name = "Unknown"
            
            for h in HATCHERIES:
                if water_name.upper().endswith(h):
                    hatchery_name = h.title().replace("Trout Rearing Facility", "TRF").replace("Hatchery (Parkview)", "Hatchery")
                    water_name = water_name[:-len(h)].strip()
                    break # Stop after first match

            # Final cleanup for common artifacts
            if hatchery_name == "Unknown" and "PRIVATE" in full_name_part.upper():
                hatchery_name = "Private"
                water_name = full_name_part.upper().replace("PRIVATE", "").strip()

            date_str = record_match.group(5)
            # Timezone-safe date parsing
            dt_object = datetime.strptime(date_str, '%m/%d/%Y')
            formatted_date = dt_object.strftime('%Y-%m-%d')
            
            record = {
                'date': formatted_date,
                'species': current_species,
                'quantity': record_match.group(4).replace(',', ''),
                'length': record_match.group(2),
                'hatchery': hatchery_name,
                'reportUrl': pdf_url
            }
            records.append(record)
            # print(f"    [+] Found Record for '{water_name}': {record['date']} - {record['species']}")

    return records

def generate_static_pages(data):
    """Generates a static HTML page for each water body."""
    print("--- Starting Static Page Generation ---")
    if not os.path.exists(TEMPLATE_FILE):
        print(f"[!] Error: Template file '{TEMPLATE_FILE}' not found.")
        return
        
    with open(TEMPLATE_FILE, 'r') as f:
        template_content = f.read()

    for water_name, water_data in data.items():
        if not water_data.get('records'):
            continue

        table_rows = ""
        # Sort records by date descending for display
        sorted_records = sorted(water_data['records'], key=lambda x: x['date'], reverse=True)
        
        for record in sorted_records:
            dt = datetime.strptime(record['date'], '%Y-%m-%d')
            display_date = dt.strftime('%-m/%-d/%Y')
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
        page_url = f"{BASE_SITE_URL}/{STATIC_PAGES_DIR}/{filename}"

        page_content = template_content.replace('{{WATER_NAME}}', water_name)
        page_content = page_content.replace('{{PAGE_URL}}', page_url)
        page_content = page_content.replace('{{TABLE_ROWS}}', table_rows)
        
        filepath = os.path.join(STATIC_PAGES_DIR, filename)
        with open(filepath, 'w') as f:
            f.write(page_content)
    print(f"  Generated {len(data)} static pages in '{STATIC_PAGES_DIR}'")
    print("--- Finished Static Page Generation ---")


def generate_sitemap(data):
    """Generates a sitemap.xml file."""
    print("--- Starting Sitemap Generation ---")
    
    urls = [BASE_SITE_URL + "/"] # Start with homepage
    for water_name in data:
        filename_safe = water_name.lower().replace('/', ' ').replace('(', '').replace(')', '').replace('&', 'and')
        filename = re.sub(r'[^a-z0-9]+', '-', filename_safe).strip('-') + ".html"
        urls.append(f"{BASE_SITE_URL}/{STATIC_PAGES_DIR}/{filename}")

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for url in urls:
        xml += '  <url>\n'
        xml += f'    <loc>{url}</loc>\n'
        xml += f'    <lastmod>{date.today().isoformat()}</lastmod>\n'
        xml += '  </url>\n'
    xml += '</urlset>'

    with open(SITEMAP_FILE, 'w') as f:
        f.write(xml)
    print(f"  Sitemap generated with {len(urls)} URLs: {SITEMAP_FILE}")
    print("--- Finished Sitemap Generation ---")


def run_scraper(rebuild=False):
    print(f"--- Starting {'One-Time Database Rebuild' if rebuild else 'Daily Scrape Job'} ---")
    
    # Always create directories to be safe
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    os.makedirs(STATIC_PAGES_DIR, exist_ok=True)

    final_data = {}
    
    if rebuild:
        print("REBUILD MODE: Starting with a clean slate.")
        final_data = {}
        all_pdf_links = get_pdf_links_for_rebuild(ARCHIVE_PAGE_URL)
    else:
        # Daily run: Load existing data first
        try:
            print(f"Loading existing data from {LIVE_DATA_URL}...")
            response = requests.get(LIVE_DATA_URL, timeout=20)
            response.raise_for_status()
            final_data = response.json()
            print("Successfully loaded live data.")
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"[!] Could not load existing data from live URL: {e}. Starting fresh, but this is unusual.")
            final_data = {}
        
        print(f"Finding PDF links on the first archive page: {ARCHIVE_PAGE_URL}...")
        all_pdf_links = get_pdf_links_from_url(ARCHIVE_PAGE_URL)
        print(f"Found {len(all_pdf_links)} PDF links on the first page.")

    # Process PDFs
    new_records_found = 0
    processed_urls_in_this_run = set()

    for pdf_url in all_pdf_links:
        # Normalize URL to ignore 'refresh' param for duplicate checking
        base_pdf_url = pdf_url.split('&refresh=')[0]

        # Check if this base URL has already been processed IN THIS RUN
        if base_pdf_url in processed_urls_in_this_run:
            continue
        
        # In daily mode, check if we've ever processed this report before
        if not rebuild:
            already_processed = False
            for water in final_data.values():
                for record in water.get('records', []):
                    if record.get('reportUrl', '').split('&refresh=')[0] == base_pdf_url:
                        already_processed = True
                        break
                if already_processed:
                    break
            if already_processed:
                continue
        
        print(f"  > Processing new report: {pdf_url}...")
        processed_urls_in_this_run.add(base_pdf_url)

        try:
            pdf_response = requests.get(pdf_url, timeout=30)
            pdf_response.raise_for_status()
            with pdfplumber.open(io.BytesIO(pdf_response.content)) as pdf:
                pdf_text = ""
                for page in pdf.pages:
                    pdf_text += page.extract_text() + "\n"

            if not pdf_text.strip():
                print(f"    [!] Warning: PDF is empty or text could not be extracted: {pdf_url}")
                continue

            parsed_records = parse_pdf_text(pdf_text, pdf_url)
            
            # This is a fallback parser for a different, older format
            if not parsed_records:
                pass # Add legacy parser logic here if needed in the future

            if parsed_records:
                new_records_found += len(parsed_records)
                for record in parsed_records:
                    # Logic to add new records to the main data structure
                    # This logic needs to be added back in.
                    pass # Placeholder

        except Exception as e:
            print(f"    [!] Failed to process PDF {pdf_url}: {e}")
        time.sleep(1)

    if new_records_found > 0:
        print(f"\nFound a total of {new_records_found} new records.")
    else:
        print("\nNo new reports to process. Data is up-to-date.")

    # Always save the data file, even if no new records, to handle data loss on build failures
    if final_data:
        # De-duplicate and sort before saving
        for water_name, water_data in final_data.items():
            if 'records' in water_data:
                # Create a unique tuple for each record to handle duplicates
                unique_recs_set = { (r['date'], r['species'], r['quantity'], r['length'], r['hatchery']) for r in water_data['records'] }
                # Rebuild the list of records from the unique set
                unique_records = [
                    {'date': t[0], 'species': t[1], 'quantity': t[2], 'length': t[3], 'hatchery': t[4], 
                     'reportUrl': next(r['reportUrl'] for r in water_data['records'] if (r['date'], r['species'], r['quantity'], r['length'], r['hatchery']) == t)}
                    for t in unique_recs_set
                ]
                final_data[water_name]['records'] = sorted(unique_records, key=lambda x: x['date'], reverse=True)
        
        try:
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"Successfully saved data file: {OUTPUT_FILE}")

            print("Proceeding to generate static pages and sitemap...")
            generate_static_pages(final_data)
            generate_sitemap(final_data)

        except IOError as e:
            print(f"[!] Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("[!] No data available to save. The data file was not written.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    is_rebuild = "--rebuild" in sys.argv
    run_scraper(rebuild=is_rebuild)

