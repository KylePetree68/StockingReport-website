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

# This is the single, definitive script for all scraping operations.

BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"
LIVE_DATA_URL = "https://stockingreport.com/stocking_data.json"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"
TEMPLATE_FILE = "template.html"
OUTPUT_DIR = "public/waters"
SITEMAP_FILE = "public/sitemap.xml"
MANUAL_COORDS_FILE = "manual_coordinates.json"

def validate_url(url, timeout=5):
    """
    Check if a URL returns a valid PDF response.
    Returns True if URL is valid and returns PDF content, False otherwise.
    """
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        # Check if status is OK and content-type suggests PDF
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'pdf' in content_type or 'application/octet-stream' in content_type:
                return True
        return False
    except:
        return False

def get_fallback_url(nmdgf_url):
    """
    Given an NMDGF URL, return the local fallback URL if the file exists.
    Example: https://wildlife.dgf.nm.gov/download/stocking-report-8-29-25/?wpdmdl=...
             -> /public/reports/stocking-report-8-29-25.pdf
    """
    try:
        # Extract filename from NMDGF URL
        parts = nmdgf_url.split('/download/')[1].split('?')[0].strip('/')
        filename = parts + '.pdf'
        local_path = os.path.join('public', 'reports', filename)

        # Check if local file exists
        if os.path.exists(local_path):
            return f"/public/reports/{filename}"
    except:
        pass
    return None

def get_pdf_links_for_rebuild(start_url):
    """
    Scrapes archive pages starting from a hardcoded year and moving forward.
    """
    target_year = 2020
    print(f"Finding all PDF links for year {target_year} and later, starting from: {start_url}...")
    all_pdf_links = []
    current_page_url = start_url
    page_count = 1
    keep_scraping = True

    while current_page_url and keep_scraping:
        print(f"  Scraping archive page {page_count}: {current_page_url}")
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            
            content_div = soup.find("div", class_="post-content")
            if not content_div:
                print(f"    Could not find content div on page {page_count}. Stopping.")
                break

            links_on_page = content_div.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE))
            if not links_on_page:
                print("    No report links found on this page. Stopping.")
                break

            for a_tag in links_on_page:
                date_match = re.search(r'(\d{1,2})[_-](\d{1,2})[_-](\d{2})', a_tag.get_text())
                if date_match:
                    report_year = int(f"20{date_match.group(3)}")
                    if report_year < target_year:
                        print(f"    Found report from {report_year}. Stopping archive scrape.")
                        keep_scraping = False
                        break 
                
                if "?wpdmdl=" in a_tag['href']:
                    full_url = a_tag['href']
                    if not full_url.startswith('http'):
                        full_url = f"{BASE_URL}{full_url}"
                    if full_url not in all_pdf_links:
                        all_pdf_links.append(full_url)
            
            if not keep_scraping: break

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

    print(f"\nFinished scraping archive. Found {len(all_pdf_links)} total PDF links for the target year.")
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
    A robust parser that handles two different PDF formats:
    - Old format (2020-2021): Water Name | Length | Weight | Number | Date | ID
    - New format (2022+): Water Name | Full Hatchery Name | Length | Weight | Number | Date | ID
    Note: In 2022 PDFs, text often wraps across multiple lines and needs to be merged.
    """
    all_records = {}
    current_species = None

    hatchery_map = {
        'LO': 'Los Ojos Hatchery (Parkview)', 'PVT': 'Private', 'RR': 'Red River Trout Hatchery',
        'LS': 'Lisboa Springs Trout Hatchery', 'RL': 'Rock Lake Trout Rearing Facility',
        'FED': 'Federal Hatchery', 'SS': 'Seven Springs Trout Hatchery', 'GW': 'Glenwood Springs Hatchery'
    }
    hatchery_names_sorted = sorted(hatchery_map.values(), key=len, reverse=True)

    species_regex = re.compile(r"^[A-Z][a-zA-Z]+(?:\s[A-Za-z\s]+)*$")

    lines = text.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1

        if not line: continue

        # Skip standalone continuation words - these should only be merged with previous lines
        # Be very specific to avoid skipping actual water names
        if line in ["FACILITY", "HATCHERY", "PRIVATE", "Beach)"]:
            continue

        # Detect species headers (but exclude hatchery keywords like "FACILITY")
        if (species_regex.match(line) and
            "By Date For" not in line and
            len(line.split()) < 5 and
            line.upper() not in ['FACILITY', 'HATCHERY', 'PRIVATE']):
            current_species = line.strip()
            continue

        # Skip header and total lines
        if line.startswith("Water Name") or line.startswith("TOTAL") or line.startswith("Stocking Report By Date"):
            continue

        # Check if next line should be merged (wrapped text continuation)
        # This needs to happen BEFORE we try to parse the record structure
        while i < len(lines):
            next_line = lines[i].strip()

            # Don't merge empty lines, headers, or species names
            if (not next_line or
                next_line.startswith("Water Name") or
                next_line.startswith("TOTAL") or
                next_line.startswith("Stocking Report") or
                (species_regex.match(next_line) and len(next_line.split()) < 5)):
                break

            # Check if current line looks like a complete record
            words = line.split()
            if len(words) >= 2:
                potential_id = words[-1]
                potential_date = words[-2]
                has_valid_ending = (re.match(r"\d{2}\/\d{2}\/\d{4}", potential_date) and
                                    potential_id in hatchery_map)
            else:
                has_valid_ending = False

            # If record is incomplete, merge next line
            if not has_valid_ending:
                line = line + " " + next_line
                i += 1
                continue

            # If record looks complete but next line is short wrapper text, merge it too
            # Examples: "Beach)", "FACILITY", or other 1-3 word continuations ending in )
            next_words = next_line.split()
            if (len(next_words) <= 3 and (next_line.endswith(')') or next_line == "FACILITY")):
                # Insert the continuation BEFORE the data fields, not after
                # Split: water_name + hatchery + length + weight + number + date + ID
                # We want to insert before length (5th from end)
                if len(words) >= 6:
                    line = " ".join(words[:-5]) + " " + next_line + " " + " ".join(words[-5:])
                    i += 1
                    continue

            # Otherwise don't merge
            break

        words = line.split()
        if len(words) < 6: continue

        try:
            hatchery_id = words[-1]
            date_str = words[-2]
            number = words[-3]

            # Validate hatchery ID and date format
            if not re.match(r"\d{2}\/\d{2}\/\d{4}", date_str): continue
            if hatchery_id not in hatchery_map: continue

            # Get length (4th from end after ID, date, number, weight)
            length = words[-5]

            # Everything before the last 5 words is the water name + possibly hatchery name
            name_and_hatchery = " ".join(words[:-5])

            # Remove the full hatchery name from the combined string (case insensitive)
            # Also remove partial matches for cases where "FACILITY" was merged separately
            water_name = name_and_hatchery
            for h_name_to_remove in hatchery_names_sorted:
                if h_name_to_remove == 'Private': continue
                # Try full match first
                if h_name_to_remove.upper() in water_name.upper():
                    idx = water_name.upper().find(h_name_to_remove.upper())
                    water_name = water_name[:idx] + water_name[idx + len(h_name_to_remove):]
                    break
                # Also try partial matches (e.g., "ROCK LAKE TROUT REARING" without "FACILITY")
                # Split hatchery name and check if most words are present
                h_words = h_name_to_remove.upper().split()
                if len(h_words) > 2:
                    # Check if at least the first N-1 words are present consecutively
                    partial = " ".join(h_words[:-1])
                    if partial in water_name.upper():
                        idx = water_name.upper().find(partial)
                        water_name = water_name[:idx] + water_name[idx + len(partial):]
                        break

            # Also handle standalone "PRIVATE" keyword
            if 'PRIVATE' in water_name.upper():
                idx = water_name.upper().find('PRIVATE')
                water_name = water_name[:idx] + water_name[idx + 7:]

            # Clean up water name: remove extra spaces and title case
            water_name = " ".join(water_name.split()).title()

            if not water_name: continue

            # Get hatchery name from ID map
            hatchery_name = hatchery_map.get(hatchery_id)

            # Format date
            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            formatted_date = date_obj.strftime("%Y-%m-%d")

            # Create record
            record = {"date": formatted_date, "species": current_species, "quantity": number.replace(',', ''), "length": length, "hatchery": hatchery_name, "reportUrl": report_url}

            if water_name not in all_records:
                all_records[water_name] = {"records": []}
            all_records[water_name]["records"].append(record)

        except (ValueError, IndexError):
            continue

    return all_records

def enrich_data_with_coordinates(data, manual_coords):
    """
    Adds latitude and longitude, prioritizing the manual override file.
    """
    print("\n--- Starting Geocoding Enrichment ---")
    enriched_count = 0
    for water_name in data.keys():
        if data[water_name].get("coords"):
            continue

        if water_name in manual_coords:
            print(f"  -> Using manual coordinates for {water_name}...")
            data[water_name]["coords"] = manual_coords[water_name]
            enriched_count += 1
            continue

        print(f"  -> Fetching coordinates for {water_name}...")
        try:
            query = f"{water_name}, New Mexico"
            url = f"https://nominatim.openstreetmap.org/search?q={requests.utils.quote(query)}&format=json&limit=1"
            headers = {'User-Agent': 'NMStockingReport/1.0'}
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            results = response.json()
            
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                data[water_name]["coords"] = {"lat": lat, "lon": lon}
                enriched_count += 1
                print(f"    [+] Found coordinates: {lat}, {lon}")
            else:
                data[water_name]["coords"] = None
                print(f"    [!] Could not find coordinates for {water_name}")
            
            time.sleep(1.5)
        except Exception as e:
            print(f"    [!] Error fetching coordinates for {water_name}: {e}")
            data[water_name]["coords"] = None
    
    print(f"Enriched {enriched_count} new water bodies with coordinates.")
    print("--- Geocoding Enrichment Finished ---")
    return data

def generate_static_pages(data):
    """
    Generates an individual HTML page for each water body.
    Validates NMDGF URLs and falls back to local copies when needed.
    """
    print("\n--- Starting Static Page Generation ---")
    if not os.path.exists(TEMPLATE_FILE):
        print(f"Error: Template file '{TEMPLATE_FILE}' not found. Cannot generate pages.")
        return

    with open(TEMPLATE_FILE, "r") as f:
        template_html = f.read()

    # Cache for URL validation to avoid checking same URL multiple times
    url_validation_cache = {}
    validated_count = 0
    fallback_count = 0

    generated_count = 0
    for water_name, water_data in data.items():
        print(f"  -> Generating page for {water_name}...")
        filename = re.sub(r'[^a-z0-9]+', '-', water_name.lower()).strip('-') + ".html"
        filepath = os.path.join(OUTPUT_DIR, filename)

        table_rows_html = ""
        for record in water_data.get("records", []):
            date_obj = datetime.strptime(record['date'], "%Y-%m-%d")
            display_date = date_obj.strftime("%b %d, %Y")

            onclick_attr = ""
            if record.get("reportUrl"):
                url = record['reportUrl']

                # If it's an NMDGF URL, validate it and potentially fall back to local
                if 'wildlife.dgf.nm.gov' in url:
                    # Check cache first
                    if url not in url_validation_cache:
                        url_validation_cache[url] = validate_url(url)

                    if url_validation_cache[url]:
                        # NMDGF URL is valid, use it
                        validated_count += 1
                    else:
                        # NMDGF URL is broken, try fallback
                        fallback = get_fallback_url(url)
                        if fallback:
                            url = fallback
                            fallback_count += 1

                onclick_attr = f"onclick=\"window.open('{url}', '_blank')\""

            table_rows_html += f"""
                <tr class="clickable-row hover:bg-gray-50" {onclick_attr}>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{display_date}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800">{record['species']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['quantity']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['length']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['hatchery']}</td>
                </tr>
            """

        page_html = template_html.replace("{{WATER_NAME}}", water_name)
        page_html = page_html.replace("{{TABLE_ROWS}}", table_rows_html)

        with open(filepath, "w") as f:
            f.write(page_html)
        generated_count += 1

    print(f"Generated {generated_count} static pages in '{OUTPUT_DIR}'.")
    print(f"URL validation: {validated_count} NMDGF URLs valid, {fallback_count} fell back to local copies")
    print("--- Static Page Generation Finished ---")

def generate_sitemap(data):
    """
    Generates a sitemap.xml file from the data.
    """
    print("\n--- Starting Sitemap Generation ---")
    
    urls = ["https://stockingreport.com/"]
    
    for water_name in data.keys():
        filename = re.sub(r'[^a-z0-9]+', '-', water_name.lower()).strip('-') + ".html"
        url = f"https://stockingreport.com/public/waters/{filename}"
        urls.append(url)

    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    
    today = date.today().isoformat()
    
    for url in urls:
        xml_content += '  <url>\n'
        xml_content += f'    <loc>{url}</loc>\n'
        xml_content += f'    <lastmod>{today}</lastmod>\n'
        xml_content += '  </url>\n'
        
    xml_content += '</urlset>'
    
    try:
        with open(SITEMAP_FILE, "w") as f:
            f.write(xml_content)
        print(f"Successfully generated sitemap with {len(urls)} URLs: {SITEMAP_FILE}")
    except IOError as e:
        print(f"Error writing sitemap file: {e}")
        
    print("--- Sitemap Generation Finished ---")

def run_scraper(rebuild=False):
    """
    Main function to orchestrate the scraping process.
    """
    if not os.path.exists("public"):
        os.makedirs("public")
        print("Created 'public' directory.")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    manual_coords = {}
    if os.path.exists(MANUAL_COORDS_FILE):
        print(f"Loading manual coordinates from {MANUAL_COORDS_FILE}...")
        with open(MANUAL_COORDS_FILE, "r") as f:
            manual_coords = json.load(f)

    if rebuild:
        print("--- Starting One-Time Database Rebuild ---")
        final_data = {}
        all_pdf_links = get_pdf_links_for_rebuild(ARCHIVE_PAGE_URL)
        if not all_pdf_links:
            print("No PDF links found. Aborting rebuild.")
            return
    else:
        print("--- Starting Daily Scrape Job ---")
        try:
            print(f"Loading existing data from {LIVE_DATA_URL}...")
            response = requests.get(LIVE_DATA_URL)
            response.raise_for_status()
            final_data = response.json()
            print("Successfully loaded live data.")
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Warning: Could not load or parse live data file. Error: {e}. Aborting to prevent data loss.")
            return
        
        processed_urls = set()
        for water_data in final_data.values():
            for record in water_data.get("records", []):
                if "reportUrl" in record:
                    processed_urls.add(record["reportUrl"].split('&refresh=')[0])
        
        all_pdf_links = get_pdf_links_from_first_page(ARCHIVE_PAGE_URL)
        new_pdf_links = [link for link in all_pdf_links if link.split('&refresh=')[0] not in processed_urls]
        
        if not new_pdf_links:
            print("\nNo new reports to process. Data is up-to-date.")
            try:
                with open(OUTPUT_FILE, "w") as f:
                    json.dump(final_data, f, indent=4)
                print("Re-saved existing data to ensure file is not empty.")
                generate_static_pages(final_data)
                generate_sitemap(final_data)
            except IOError as e:
                print(f"Error re-saving data file: {e}")
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
                    existing_records_set = {json.dumps(rec, sort_keys=True) for rec in final_data[water_body]['records']}
                    for new_record in data['records']:
                        new_record_str = json.dumps(new_record, sort_keys=True)
                        if new_record_str not in existing_records_set:
                            final_data[water_body]['records'].append(new_record)
        time.sleep(1)
    
    print("\nScrape complete. Saving data...")
    
    if final_data:
        # **THE FIX IS HERE**: The call to enrich data with coordinates is restored.
        final_data = enrich_data_with_coordinates(final_data, manual_coords)

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

            print("Proceeding to generate static pages and sitemap...")
            generate_static_pages(final_data)
            generate_sitemap(final_data)
            print("Static pages and sitemap generation complete.")

        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("No data was parsed. The data file was not written.")

    print("--- Scrape Job Finished ---")

if __name__ == "__main__":
    if "--rebuild" in sys.argv:
        run_scraper(rebuild=True)
    else:
        run_scraper(rebuild=False)
