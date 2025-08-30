import requests
from bs4 import BeautifulSoup
import pdfplumber
import json
import os
import re
from datetime import datetime

# --- Configuration ---
DATA_FILE = 'stocking_data.json'
MANUAL_COORDS_FILE = 'manual_coordinates.json'
ARCHIVE_PAGE_URL = 'https://wildlife.dgf.nm.gov/fishing/weekly-report/fish-stocking-archive/'
SITE_BASE_URL = 'https://stockingreport.com'
TARGET_YEAR = 2025 # The year to start the historical data rebuild from

# --- Main Scraper Logic ---

def get_pdf_links_from_single_page(page_url):
    """Fetches all 'Stocking Report' PDF links from a single archive page."""
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the main content area of the page
        content_div = soup.find('div', class_='inside-article')
        if not content_div:
            print(f"  [!] Could not find content div on page {page_url}. Stopping.")
            return []
            
        links = []
        for a in content_div.find_all('a', href=True):
            if 'Stocking Report' in a.text:
                links.append(a['href'])
        return links
    except requests.exceptions.RequestException as e:
        print(f"  [!] Error fetching page {page_url}: {e}")
        return []

def get_pdf_links_for_rebuild(start_page_url):
    """Fetches all PDF links from all pages of the archive for a full rebuild."""
    all_links = []
    page_num = 1
    next_page_url = start_page_url
    
    while next_page_url:
        print(f"  Scraping archive page {page_num}: {next_page_url}")
        
        try:
            response = requests.get(next_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            content_div = soup.find('div', class_='inside-article')
            if not content_div:
                print(f"  [!] Could not find content div on page {page_num}. Stopping.")
                break

            page_links = []
            stop_scraping = False
            for a in content_div.find_all('a', href=True):
                # Extract year from the link text to stop at the target year
                match = re.search(r'(\d{1,2})-(\d{1,2})-(\d{2,4})', a.text)
                if match:
                    year_str = match.group(3)
                    year = int("20" + year_str) if len(year_str) == 2 else int(year_str)
                    if year < TARGET_YEAR:
                        stop_scraping = True
                        print(f"  Found report from {year}, stopping rebuild scan.")
                        break
                
                if 'Stocking Report' in a.text:
                    page_links.append(a['href'])
            
            all_links.extend(page_links)
            if stop_scraping:
                break

            # Find the "Next" link to go to the next page
            next_link = soup.find('a', class_='next page-numbers')
            next_page_url = next_link['href'] if next_link else None
            page_num += 1

        except requests.exceptions.RequestException as e:
            print(f"  [!] Error fetching page {next_page_url}: {e}")
            break
            
    print(f"\nFinished scraping archive. Found {len(all_links)} total PDF links for {TARGET_YEAR} and forward.")
    return all_links

def parse_pdf_text(pdf_text, report_url):
    """Parses text extracted from a PDF to find stocking records."""
    records = []
    current_species = None
    lines = pdf_text.split('\n')
    
    species_regex = re.compile(r'^[A-Z][a-zA-Z\s]+$')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if the line is a species header
        if species_regex.match(line) and "TOTAL" not in line and "Hatchery" not in line:
            # More specific check to avoid false positives
            words = line.split()
            if len(words) < 4: # A species name is unlikely to be more than 3 words
                current_species = line.title()
                print(f"    [Parser] Species context set to: {current_species}")
                continue
        
        # Regex to capture a stocking record
        # This is more robust to handle variations in spacing
        record_regex = re.compile(
            r'^(.*?)\s+'  # Water Name (non-greedy)
            r'([\d,]+\.?\d*)\s+'  # Length
            r'([\d,]+\.?\d*)\s+'  # Lbs
            r'([\d,]+)\s+'  # Number
            r'(\d{2}\/\d{2}\/\d{4})\s+'  # Date
            r'([A-Z\s]+)$'  # Hatchery ID
        )

        match = record_regex.search(line)
        if match and current_species:
            water_name_raw = match.group(1).strip()
            
            # --- Name Cleaning Logic ---
            hatcheries = ["LOS OJOS HATCHERY (PARKVIEW)", "RED RIVER TROUT HATCHERY", "LISBOA SPRINGS TROUT HATCHERY", "ROCK LAKE TROUT REARING FACILITY", "SEVEN SPRINGS TROUT HATCHERY", "PRIVATE", "FEDERAL HATCHERY"]
            
            cleaned_name = water_name_raw
            hatchery_name = ""

            # Find which hatchery is in the name part
            for h in hatcheries:
                if h in water_name_raw.upper():
                    hatchery_name = h.title()
                    # Remove the hatchery name from the water name
                    cleaned_name = water_name_raw.upper().replace(h, "").strip()
                    break
            
            # Final cleanup for any leftover characters
            cleaned_name = cleaned_name.replace(")", "").replace("(", "").strip().title()

            if not hatchery_name:
                 hatchery_name = "N/A" # Fallback

            try:
                # Format date to YYYY-MM-DD
                date_obj = datetime.strptime(match.group(5), '%m/%d/%Y')
                stock_date = date_obj.strftime('%Y-%m-%d')

                record = {
                    'water_name': cleaned_name,
                    'date': stock_date,
                    'species': current_species,
                    'quantity': match.group(4).replace(',', ''),
                    'length': match.group(2).replace(',', ''),
                    'hatchery': hatchery_name,
                    'reportUrl': report_url
                }
                records.append(record)
                print(f"    [+] Found Record for '{record['water_name']}': {record['date']} - {record['species']}")

            except ValueError:
                print(f"    [!] Could not parse date for line: {line}")
    return records


def run_scraper(rebuild=False):
    """Main function to run the scraper."""
    
    if rebuild:
        print("\n--- Starting One-Time Database Rebuild ---")
        print(f"This will process all reports from {TARGET_YEAR} onward.")
        final_data = {}
        all_pdf_links = get_pdf_links_for_rebuild(ARCHIVE_PAGE_URL)
    else:
        print("\n--- Starting Daily Scrape Job ---")
        # Load existing data from the live URL to prevent data loss
        try:
            print(f"Loading existing data from {SITE_BASE_URL}/{DATA_FILE}...")
            live_data_url = f"{SITE_BASE_URL}/{DATA_FILE}?v={datetime.now().timestamp()}"
            response = requests.get(live_data_url)
            response.raise_for_status()
            final_data = response.json()
            print("Successfully loaded live data.")
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"[!] Could not load or parse live data file: {e}. Starting with an empty database.")
            final_data = {}
        
        print(f"Finding PDF links on the first archive page: {ARCHIVE_PAGE_URL}...")
        all_pdf_links = get_pdf_links_from_single_page(ARCHIVE_PAGE_URL)
        print(f"Found {len(all_pdf_links)} PDF links on the first page.")

    # --- Process PDFs ---
    new_records_found = 0
    processed_urls = {record['reportUrl'].split('&refresh=')[0] for water in final_data.values() for record in water.get('records', [])}

    for pdf_url in all_pdf_links:
        base_url = pdf_url.split('&refresh=')[0]
        if not rebuild and base_url in processed_urls:
            continue
            
        print(f"  > Processing {pdf_url}...")
        try:
            with requests.get(pdf_url, stream=True) as r:
                r.raise_for_status()
                with pdfplumber.open(r.raw) as pdf:
                    full_text = "".join(page.extract_text() for page in pdf.pages if page.extract_text())
            
            if full_text:
                new_records = parse_pdf_text(full_text, pdf_url)
                for rec in new_records:
                    water = rec['water_name']
                    if water not in final_data:
                        final_data[water] = {'records': [], 'coords': None}
                    
                    # Prevent duplicates
                    is_duplicate = False
                    for existing_rec in final_data[water]['records']:
                        if (existing_rec['date'] == rec['date'] and 
                            existing_rec['species'] == rec['species'] and
                            existing_rec['quantity'] == rec['quantity']):
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        final_data[water]['records'].append(rec)
                        new_records_found += 1
                processed_urls.add(base_url)
            else:
                 print(f"    [!] Could not extract text from {pdf_url}. It might be an image-based PDF.")

        except Exception as e:
            print(f"    [!] Failed to process {pdf_url}: {e}")

    if new_records_found > 0 or rebuild:
        print(f"\nFound a total of {new_records_found} new records.")
    else:
        print("\nNo new reports to process. Data is up-to-date.")

    # --- Data Enrichment (Coordinates) ---
    print("\n--- Starting Data Enrichment (Coordinates) ---")
    try:
        with open(MANUAL_COORDS_FILE, 'r') as f:
            manual_coords = json.load(f)
        print("Loaded manual coordinates file.")
    except (FileNotFoundError, json.JSONDecodeError):
        manual_coords = {}
        print("No manual coordinates file found or file is invalid. Proceeding with automatic lookup.")

    for water_name, data in final_data.items():
        if not data.get('coords'):
            if water_name in manual_coords:
                data['coords'] = manual_coords[water_name]
                print(f"  -> Applied manual coordinates for {water_name}")
            else:
                # Fetch coords automatically if not in manual file
                try:
                    search_url = f"https://nominatim.openstreetmap.org/search?q={water_name}, New Mexico&format=json&limit=1"
                    headers = {'User-Agent': 'NMStockingReport/1.0'}
                    response = requests.get(search_url, headers=headers)
                    response.raise_for_status()
                    geo_data = response.json()
                    if geo_data:
                        lat = float(geo_data[0]['lat'])
                        lon = float(geo_data[0]['lon'])
                        data['coords'] = {'lat': lat, 'lon': lon}
                        print(f"  -> Successfully fetched coordinates for {water_name}")
                    else:
                        print(f"  [!] Could not find coordinates for {water_name}")
                except Exception as e:
                    print(f"  [!] Error fetching coordinates for {water_name}: {e}")

    # --- Generate Static Pages and Sitemap ---
    generate_static_pages(final_data)

    # --- Final Save ---
    # Always save the file to ensure data integrity on Render
    try:
        # Sort records within each water body by date descending
        for water in final_data.values():
            water['records'].sort(key=lambda r: r['date'], reverse=True)
            
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=4)
        print(f"\nSuccessfully updated data file: {DATA_FILE}")
    except Exception as e:
        print(f"\n[!!!] CRITICAL ERROR: Could not write to data file: {e}")

    print("\n--- Scrape Job Finished ---")


def generate_static_pages(data):
    """Generates a static HTML page for each water body."""
    print("\n--- Generating Static HTML Pages ---")
    if not os.path.exists('public/waters'):
        os.makedirs('public/waters')
    
    try:
        with open('template.html', 'r') as f:
            template = f.read()
    except FileNotFoundError:
        print("[!] template.html not found. Cannot generate static pages.")
        return

    sitemap_urls = [SITE_BASE_URL + '/']
    
    for water_name, water_data in data.items():
        print(f"  Generating page for: {water_name}")
        file_name = water_name.lower().replace(' ', '-').replace('(', '').replace(')', '').replace('/', '-') + ".html"
        page_url = f"{SITE_BASE_URL}/public/waters/{file_name}"
        sitemap_urls.append(page_url)

        table_rows = ""
        # Ensure records are sorted by date for display
        sorted_records = sorted(water_data.get('records', []), key=lambda r: r['date'], reverse=True)
        
        for record in sorted_records:
            date_obj = datetime.strptime(record['date'], '%Y-%m-%d')
            display_date = date_obj.strftime('%-m/%-d/%Y')
            table_rows += f"""
            <tr class="clickable-row" onclick="window.open('{record['reportUrl']}', '_blank')">
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{display_date}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800">{record['species']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['quantity']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['length']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['hatchery']}</td>
            </tr>
            """
        
        page_content = template.replace('{{WATER_NAME}}', water_name)
        page_content = page_content.replace('{{TABLE_ROWS}}', table_rows)
        page_content = page_content.replace('{{PAGE_URL}}', page_url)
        
        with open(os.path.join('public/waters', file_name), 'w') as f:
            f.write(page_content)

    print(f"Generated {len(data)} static pages.")
    generate_sitemap(sitemap_urls)

def generate_sitemap(urls):
    """Generates a sitemap.xml file from a list of URLs."""
    print("\n--- Generating sitemap.xml ---")
    if not os.path.exists('public'):
        os.makedirs('public')
        
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for url in urls:
        xml_content += '  <url>\n'
        xml_content += f'    <loc>{url}</loc>\n'
        xml_content += '  </url>\n'
    xml_content += '</urlset>'
    
    with open('public/sitemap.xml', 'w') as f:
        f.write(xml_content)
    print("Successfully generated sitemap.xml")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='NM Stocking Report Scraper')
    parser.add_argument('--rebuild', action='store_true', help='Perform a full rebuild of the database from the archive.')
    args = parser.parse_args()

    run_scraper(rebuild=args.rebuild)
