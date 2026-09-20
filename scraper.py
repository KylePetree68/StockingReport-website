import requests
import html as html_lib
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, date, timedelta
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
WATER_IMAGES_FILE  = "water_images.json"

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

def is_valid_length(length_str):
    """Return True if length looks like a real fish measurement (numeric or range like 8-10)."""
    if not length_str:
        return False
    s = str(length_str).strip()
    if re.match(r'^\d+(\.\d+)?$', s):
        return True
    if re.match(r'^\d+(\.\d+)?-\d+(\.\d+)?$', s):
        return True
    return False

TESSERACT_CMD = r'C:\Users\kyle\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

def _is_garbled(text):
    """Return True if extracted text contains mostly (cid:XX) encoding artifacts."""
    if not text:
        return True
    cid_count = text.count('(cid:')
    total_chars = len(text)
    return total_chars > 0 and (cid_count / total_chars) > 0.05

def _ocr_pdf(pdf_bytes):
    """Render PDF pages to images and OCR them. Used as fallback for garbled PDFs."""
    try:
        import pytesseract
        from PIL import Image as PILImage
        if os.path.exists(TESSERACT_CMD):
            pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
        full_text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                img = page.to_image(resolution=200).original
                page_text = pytesseract.image_to_string(img, config='--psm 4')
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] OCR fallback failed: {e}")
        return ""

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    Falls back to OCR if the PDF uses an unreadable font encoding.
    """
    print(f"  > Processing {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()
        pdf_bytes = response.content
        full_text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=False)
                if page_text:
                    full_text += page_text + "\n"

        if _is_garbled(full_text):
            print(f"    [!] Garbled text detected (custom font encoding), falling back to OCR...")
            full_text = _ocr_pdf(pdf_bytes)
            if full_text:
                print(f"    [+] OCR succeeded")

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

            # Reject malformed water names: starts with a number, or starts with
            # a partial hatchery fragment like "(Parkview)", "Beach)", "State Park)"
            if re.match(r'^\d|^\(|^[A-Za-z]+\)', water_name):
                print(f"    Skipping malformed water name: {water_name!r}")
                continue

            # Normalize truncated water names to their canonical form
            WATER_NAME_ALIASES = {
                'Conservancy Park Lake (Aka Tingley': 'Conservancy Park Lake (Aka Tingley Beach)',
                'Conservancy Park Lake (Aka Tingley Beach': 'Conservancy Park Lake (Aka Tingley Beach)',
                'Pecos River (South San Isidro To Villanu': 'Pecos River (Vill Of Pecos - Villanueva)',
                'Pecos River (South San Isidro To Villanueva': 'Pecos River (Vill Of Pecos - Villanueva)',
                'Pecos River (South San Isidro To Villanueva State Park)': 'Pecos River (Vill Of Pecos - Villanueva)',
                "Rock Lake Hatchery Kid'S Pond'S (Near Ro": 'Rock Lake Hatchery Kids Ponds (Near Roswell)',
            }
            water_name = WATER_NAME_ALIASES.get(water_name, water_name)

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
        # Human-verified coordinates always win, even over an existing pin,
        # so a bad geocode can be corrected by editing manual_coordinates.json.
        if water_name in manual_coords and isinstance(manual_coords[water_name], dict):
            m = manual_coords[water_name]
            fixed = {"lat": m["lat"], "lon": m["lon"]}
            if data[water_name].get("coords") != fixed:
                print(f"  -> Using manual coordinates for {water_name}...")
                data[water_name]["coords"] = fixed
                enriched_count += 1
            continue

        if data[water_name].get("coords"):
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

def generate_summary_stats(records):
    """
    Generate summary statistics from stocking records, including recent activity.

    Args:
        records: List of stocking records

    Returns:
        Dict containing summary statistics with both recent (6-month) and lifetime data
    """
    if not records:
        return None

    total_stockings = len(records)

    # Parse all dates
    dated_records = []
    for r in records:
        try:
            d = datetime.strptime(r['date'], '%Y-%m-%d')
            dated_records.append((d, r))
        except (ValueError, KeyError):
            pass

    if not dated_records:
        return None

    dated_records.sort(key=lambda x: x[0], reverse=True)
    most_recent_date = dated_records[0][0]
    earliest_date = dated_records[-1][0]

    # --- Recent stats (last 6 months from today) ---
    today = datetime.now()
    six_months_ago = today - timedelta(days=182)
    recent_records = [(d, r) for d, r in dated_records if d >= six_months_ago]

    recent_species_counts = {}
    recent_fish = 0
    recent_lengths = []
    for d, r in recent_records:
        species = r.get('species', 'Unknown')
        recent_species_counts[species] = recent_species_counts.get(species, 0) + 1
        try:
            recent_fish += int(r.get('quantity', 0))
        except (ValueError, TypeError):
            pass
        length = r.get('length', '')
        if '-' in str(length):
            try:
                parts = str(length).split('-')
                recent_lengths.append((float(parts[0]) + float(parts[1])) / 2)
            except (ValueError, IndexError):
                pass
        else:
            try:
                recent_lengths.append(float(length))
            except (ValueError, TypeError):
                pass

    recent_avg_length = round(sum(recent_lengths) / len(recent_lengths), 1) if recent_lengths else None

    # --- Lifetime stats ---
    species_counts = {}
    total_fish = 0
    hatcheries = set()
    for d, r in dated_records:
        species = r.get('species', 'Unknown')
        species_counts[species] = species_counts.get(species, 0) + 1
        try:
            total_fish += int(r.get('quantity', 0))
        except (ValueError, TypeError):
            pass
        hatchery = r.get('hatchery')
        if hatchery:
            hatcheries.add(hatchery)

    # --- Days since last stocking ---
    days_since_last = (today - most_recent_date).days

    # --- Average days between stockings (all records, need 2+) ---
    avg_days_between = None
    if len(dated_records) >= 2:
        unique_dates = sorted(set(d for d, r in dated_records))
        if len(unique_dates) >= 2:
            gaps = [(unique_dates[i+1] - unique_dates[i]).days for i in range(len(unique_dates)-1)]
            avg_days_between = round(sum(gaps) / len(gaps))

    # --- Peak months (top 3 by stocking count) ---
    month_counts = {}
    for d, r in dated_records:
        month_counts[d.month] = month_counts.get(d.month, 0) + 1
    sorted_months = sorted(month_counts.items(), key=lambda x: x[1], reverse=True)
    peak_months = [m for m, _ in sorted_months[:3]]

    # --- Lifetime avg length ---
    all_lengths = []
    for d, r in dated_records:
        length = r.get('length', '')
        if '-' in str(length):
            try:
                parts = str(length).split('-')
                all_lengths.append((float(parts[0]) + float(parts[1])) / 2)
            except (ValueError, IndexError):
                pass
        else:
            try:
                all_lengths.append(float(length))
            except (ValueError, TypeError):
                pass
    lifetime_avg_length = round(sum(all_lengths) / len(all_lengths), 1) if all_lengths else None

    return {
        'total_stockings': total_stockings,
        'total_fish': total_fish,
        'species_counts': species_counts,
        'hatcheries': sorted(list(hatcheries)),
        'most_recent': most_recent_date.strftime('%Y-%m-%d'),
        'earliest': earliest_date.strftime('%Y-%m-%d'),
        'recent_stockings': len(recent_records),
        'recent_fish': recent_fish,
        'recent_species_counts': recent_species_counts,
        'recent_avg_length': recent_avg_length,
        'days_since_last': days_since_last,
        'avg_days_between': avg_days_between,
        'peak_months': peak_months,
        'lifetime_avg_length': lifetime_avg_length,
    }

def generate_summary_html(water_name, stats, reg_species=None, booklet_species=None, advisory_url=None):
    """
    Generate HTML summary with recent activity up top, compact historical below.

    Args:
        water_name: Name of the water body
        stats: Dict of summary statistics
        reg_species: Species from ArcGIS regulation data (trout_present field)
        booklet_species: Species from NMDGF fishing rules booklet (water_species.json)
        advisory_url: URL to the consumption advisory page in the NMDGF regulations PDF

    Returns:
        HTML string with summary
    """
    if not stats:
        return ""

    html = '<div class="mb-6 bg-gradient-to-r from-green-50 to-blue-50 border-l-4 border-green-500 p-6 rounded-r-lg">'

    # Format dates
    most_recent_str = ""
    try:
        most_recent_str = datetime.strptime(stats['most_recent'], '%Y-%m-%d').strftime('%B %d, %Y')
    except:
        most_recent_str = stats.get('most_recent', '')

    earliest_year = ""
    try:
        earliest_year = datetime.strptime(stats['earliest'], '%Y-%m-%d').strftime('%Y')
    except:
        earliest_year = stats.get('earliest', '')

    # --- Freshness badge ---
    days = stats.get('days_since_last', 9999)
    if days == 0:
        fc = "bg-green-100 text-green-800 border-green-300"
        fl = "Stocked Today — Prime Fishing Window!"
    elif days <= 7:
        fc = "bg-green-100 text-green-800 border-green-300"
        fl = f"Stocked {days} day{'s' if days > 1 else ''} ago — Prime Fishing Window"
    elif days <= 21:
        fc = "bg-yellow-100 text-yellow-800 border-yellow-300"
        fl = f"Stocked {days} days ago — Fish Are Active"
    elif days <= 60:
        fc = "bg-orange-100 text-orange-800 border-orange-300"
        fl = f"Stocked {days} days ago — Worth a Trip"
    elif days <= 182:
        months = round(days / 30)
        fc = "bg-gray-100 text-gray-600 border-gray-300"
        fl = f"Last stocked ~{months} month{'s' if months > 1 else ''} ago"
    else:
        fc = "bg-gray-100 text-gray-500 border-gray-300"
        fl = f"Last stocked {most_recent_str}"
    html += f'<div class="inline-block px-4 py-2 rounded-full border text-sm font-semibold mb-3 {fc}">{fl}</div>'

    # --- Recent activity (one line) ---
    if stats['recent_stockings'] > 0:
        recent_species = stats['recent_species_counts']
        if len(recent_species) == 1:
            sp_name = list(recent_species.keys())[0]
            fish_str = f"<strong>{stats['recent_fish']:,} {sp_name}</strong>"
        else:
            sorted_sp = sorted(recent_species.items(), key=lambda x: x[1], reverse=True)
            sp_names = [sp for sp, _ in sorted_sp]
            if len(sp_names) == 2:
                sp_joined = f"{sp_names[0]} and {sp_names[1]}"
            else:
                sp_joined = ", ".join(sp_names[:-1]) + f", and {sp_names[-1]}"
            fish_str = f"<strong>{stats['recent_fish']:,} fish</strong> ({sp_joined})"

        stat_parts = [f"<strong>{stats['recent_stockings']}</strong> stocking{'s' if stats['recent_stockings'] > 1 else ''} in the past 6 months", fish_str]
        if stats.get('recent_avg_length'):
            stat_parts.append(f"avg <strong>{stats['recent_avg_length']:.1f} in.</strong>")
        html += '<p class="text-gray-700 mt-1">' + " &nbsp;·&nbsp; ".join(stat_parts) + '</p>'
    else:
        html += f'<p class="text-gray-500 mt-1">No stockings in the past 6 months — last stocked <strong>{most_recent_str}</strong>.</p>'

    # --- Compact stats line: frequency + peak + history ---
    stats_parts = []

    avg_days = stats.get('avg_days_between')
    if avg_days:
        stats_parts.append(f"~{avg_days}-day intervals")

    peak_months = stats.get('peak_months', [])
    if peak_months:
        mn = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
              7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
        stats_parts.append("Peak: <strong>" + ", ".join(mn[m] for m in peak_months) + "</strong>")

    stats_parts.append(f"{stats['total_stockings']:,} stockings since {earliest_year}")
    stats_parts.append(f"{stats['total_fish']:,} fish")

    html += '<p class="text-sm text-gray-400 mt-2 pt-2 border-t border-gray-200">' + " &nbsp;·&nbsp; ".join(stats_parts) + '</p>'

    # --- Species Present ---
    stocked_species = sorted(stats.get('species_counts', {}).keys())
    # Normalize for deduplication: lowercase and strip trailing 's' for singular/plural matching
    def _norm(s):
        return s.lower().rstrip('s')
    stocked_norm = {_norm(s) for s in stocked_species}

    def _already_covered(name, existing_norm_set):
        n = _norm(name)
        return n in existing_norm_set

    # Parse reg_species (comma-separated string or list) from ArcGIS trout_present field
    wild_species = []
    if reg_species:
        if isinstance(reg_species, str):
            candidates = [s.strip().title() for s in reg_species.split(',')]
        else:
            candidates = [s.strip().title() for s in reg_species]
        wild_species = [s for s in candidates if not _already_covered(s, stocked_norm)]

    # Merge booklet_species from water_species.json, deduplicating against stocked + reg
    if booklet_species:
        existing_norm = stocked_norm | {_norm(s) for s in wild_species}
        for sp in booklet_species:
            sp_title = sp.strip().title()
            if not _already_covered(sp_title, existing_norm):
                wild_species.append(sp_title)
                existing_norm.add(_norm(sp_title))
    wild_species = sorted(wild_species)

    if stocked_species or wild_species:
        html += '<div class="mt-4 pt-3 border-t border-gray-200">'
        html += '<p class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Species Present</p>'
        html += '<div class="flex flex-wrap gap-2">'
        for sp in stocked_species:
            html += f'<span class="px-3 py-1 bg-blue-100 text-blue-800 text-xs font-medium rounded-full" title="Stocked by NMDGF">{sp}</span>'
        for sp in wild_species:
            html += f'<span class="px-3 py-1 bg-green-100 text-green-800 text-xs font-medium rounded-full" title="Present per fishing regulations">{sp} ✦</span>'
        html += '</div>'
        if wild_species:
            html += '<p class="text-xs text-gray-400 mt-1">✦ Listed in fishing regulations (not stocked)</p>'
        if advisory_url:
            html += f'<p class="text-xs mt-2"><a href="{advisory_url}" target="_blank" rel="noopener noreferrer" class="text-red-600 hover:underline font-medium">Consumption Advisory</a></p>'
        html += '</div>'
    elif advisory_url:
        html += f'<div class="mt-4 pt-3 border-t border-gray-200"><p class="text-xs"><a href="{advisory_url}" target="_blank" rel="noopener noreferrer" class="text-red-600 hover:underline font-medium">Consumption Advisory</a></p></div>'

    html += '</div>'
    return html

def generate_meta_description(water_name, stats):
    """
    Generate SEO meta description focused on recent activity.

    Args:
        water_name: Name of the water body
        stats: Dict of summary statistics

    Returns:
        Meta description string (max 160 characters recommended)
    """
    if not stats:
        return f"Complete stocking history for {water_name} in New Mexico. View dates, species, and quantities."

    primary_species = ""
    if stats['recent_species_counts']:
        primary_species = max(stats['recent_species_counts'].items(), key=lambda x: x[1])[0]
    elif stats['species_counts']:
        primary_species = max(stats['species_counts'].items(), key=lambda x: x[1])[0]

    most_recent_str = ""
    try:
        most_recent_str = datetime.strptime(stats['most_recent'], '%Y-%m-%d').strftime('%b %d, %Y')
    except:
        most_recent_str = stats['most_recent']

    if stats['recent_stockings'] > 0:
        description = f"{water_name}: stocked {stats['recent_stockings']} times in the last 6 months"
        if primary_species:
            description += f" with {primary_species}"
        description += f". Last stocked {most_recent_str}."
    else:
        description = f"{water_name}: last stocked {most_recent_str}."
        if primary_species:
            description += f" {stats['total_stockings']} total stockings of {primary_species}."

    description += " View complete NM stocking history."

    if len(description) > 160:
        description = description[:157] + "..."

    return description

def generate_schema_org(water_name, stats, coords, page_url):
    """
    Generate Schema.org JSON-LD structured data for a water body page.
    Produces a Dataset (stocking records) + BreadcrumbList.
    """
    import json as _json

    species_list = sorted(stats['species_counts'].keys()) if stats else []
    keywords = ["fish stocking", "New Mexico", water_name, "NMDGF", "fishing"] + species_list

    dataset = {
        "@type": "Dataset",
        "name": f"{water_name} Fish Stocking Records",
        "description": f"Complete fish stocking history for {water_name} in New Mexico, sourced from the NM Department of Game and Fish. Includes dates, species, quantities, fish length, and hatchery sources from 2020 to present.",
        "url": page_url,
        "keywords": keywords,
        "isAccessibleForFree": True,
        "creator": {
            "@type": "Organization",
            "name": "StockingReport.com",
            "url": "https://stockingreport.com"
        },
        "provider": {
            "@type": "Organization",
            "name": "New Mexico Department of Game and Fish",
            "url": "https://wildlife.dgf.nm.gov"
        },
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "spatialCoverage": {
            "@type": "Place",
            "name": water_name,
            "address": {
                "@type": "PostalAddress",
                "addressRegion": "NM",
                "addressCountry": "US"
            }
        }
    }

    if coords and coords.get('lat') and coords.get('lng'):
        dataset["spatialCoverage"]["geo"] = {
            "@type": "GeoCoordinates",
            "latitude": coords['lat'],
            "longitude": coords['lng']
        }

    if stats:
        dataset["temporalCoverage"] = f"{stats['earliest']}/{stats['most_recent']}"
        if stats['total_stockings']:
            dataset["variableMeasured"] = [
                {"@type": "PropertyValue", "name": "Total Stockings", "value": stats['total_stockings']},
                {"@type": "PropertyValue", "name": "Total Fish Stocked", "value": stats['total_fish']}
            ]

    breadcrumb = {
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://stockingreport.com"},
            {"@type": "ListItem", "position": 2, "name": water_name, "item": page_url}
        ]
    }

    schema = {
        "@context": "https://schema.org",
        "@graph": [dataset, breadcrumb]
    }

    return f'<script type="application/ld+json">\n{_json.dumps(schema, indent=2)}\n</script>'


# Hand-written, first-person notes per water. Lives in water_notes.json, which
# the scraper never writes, so the nightly Action can't clobber the prose.
WATER_NOTES_FILE = "water_notes.json"

# Who manages public access at each water (state park, national forest, city,
# BLM, ...). Hand-maintained in water_authority.json with a confidence level;
# only high/medium entries are rendered.
WATER_AUTHORITY_FILE = "water_authority.json"

# Boating / motor restrictions from the NMDGF rules booklet (hand-maintained).
BOAT_RULES_FILE = "boat_rules.json"

AUTHORITY_CATEGORY_LABELS = {
    "state_park": "State park",
    "usfs": "National Forest",
    "blm": "BLM",
    "usace": "Army Corps of Engineers",
    "usbr": "Bureau of Reclamation",
    "nps": "National Park Service",
    "usfws": "National Wildlife Refuge",
    "nmdgf": "NM Game & Fish",
    "municipal": "City / town",
    "county": "County",
    "university": "University",
    "tribal": "Tribal",
    "private": "Private",
    "mixed": "Mixed ownership",
}


def generate_water_authority_html(water_name, water_authority):
    """
    Generate the "Managed by" strip from water_authority.json.

    Renders only when confidence is high or medium. Low-confidence and
    unknown entries return "" so nothing unverified reaches the page.
    """
    import html as _html

    info = water_authority.get(water_name)
    if not isinstance(info, dict):
        return ""
    if info.get("confidence") not in ("high", "medium"):
        return ""
    authority = (info.get("authority") or "").strip()
    if not authority or info.get("category") == "unknown":
        return ""

    unit = (info.get("unit") or "").strip()
    url = (info.get("url") or "").strip()
    notes = (info.get("notes") or "").strip()
    category = info.get("category", "")
    is_mixed = category == "mixed"

    parts = []
    parts.append('<div class="mb-6 bg-gray-50 border border-gray-200 p-4 rounded-lg text-sm">')
    parts.append('<p class="text-gray-800">')
    parts.append('<span class="font-semibold text-gray-700">Managed by:</span> ')
    parts.append(f'<strong>{_html.escape(authority)}</strong>')
    if unit and unit.lower() != authority.lower():
        parts.append(f' <span class="text-gray-600">&middot; {_html.escape(unit)}</span>')
    if url and url.startswith("http"):
        parts.append(f' <a href="{_html.escape(url, quote=True)}" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:underline whitespace-nowrap">Official site &#8599;</a>')
    parts.append('</p>')
    if notes:
        # Mixed-ownership rivers always carry a note saying which stretch is public.
        parts.append(f'<p class="text-gray-600 mt-1">{_html.escape(notes)}</p>')
    if is_mixed and not notes:
        parts.append('<p class="text-gray-600 mt-1">This water crosses several ownerships, including private land. Check posting before you fish.</p>')
    parts.append('</div>')
    return "\n".join(parts)


# Daily reservoir levels (fetch_lake_levels.py), NM State Parks alerts
# (fetch_park_alerts.py) and waters that get a page without being stocked.
LAKE_LEVELS_FILE = "lake_levels.json"
PARK_ALERTS_FILE = "park_alerts.json"
EXTRA_WATERS_FILE = "extra_waters.json"

AGENCY_LINKS = {
    "usgs": "https://waterdata.usgs.gov/",
    "usbr": "https://www.usbr.gov/uc/water/hydrodata/",
    "usace": "https://water.usace.army.mil/",
}


def _fmt_level(v, unit):
    return f"{v:,.0f} {unit}" if unit == "ac-ft" else f"{v:,.1f} {unit}"


def _level_svg(series, unit, W, H, L, R, T, B, compact):
    """Line chart of (date, value) pairs. compact=True draws a sparkline with no axes."""
    import html as _html
    from datetime import datetime as _dt
    vals = [v for _, v in series]
    hi, lo = max(vals), min(vals)
    pw, ph = W - L - R, H - T - B
    pad = max((hi - lo) * 0.08, 0.5 if unit == "ft" else max(hi * 0.01, 1))
    ymin, ymax = lo - pad, hi + pad
    x0 = _dt.strptime(series[0][0], "%Y-%m-%d")
    last_date = _dt.strptime(series[-1][0], "%Y-%m-%d")
    span_days = max((last_date - x0).days, 1)

    def X(d):
        return L + pw * ((_dt.strptime(d, "%Y-%m-%d") - x0).days / span_days)

    def Y(v):
        return T + ph * (1 - (v - ymin) / (ymax - ymin))

    pts = " ".join(f"{X(d):.1f},{Y(v):.1f}" for d, v in series)
    area = f"M{X(series[0][0]):.1f},{T + ph:.1f} L" + pts.replace(" ", " L") + f" L{X(series[-1][0]):.1f},{T + ph:.1f} Z"
    cx, cy = X(series[-1][0]), Y(vals[-1])
    body = ""
    if not compact:
        ticks = []
        m = _dt(x0.year, x0.month, 1)
        while m <= last_date:
            if m >= x0:
                ticks.append(m)
            m = _dt(m.year + (m.month == 12), 1 if m.month == 12 else m.month + 1, 1)
        for i, m in enumerate(ticks):
            x = X(m.strftime("%Y-%m-%d"))
            label = m.strftime("%b") + (m.strftime(" %y") if m.month == 1 else "")
            body += f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T + ph}" stroke="#e5e7eb" stroke-width="1"/>'
            if len(ticks) <= 13 or i % 2 == 0:
                body += f'<text x="{x:.1f}" y="{H - 10}" font-size="11" fill="#6b7280" text-anchor="middle">{label}</text>'
        for v in (lo, (lo + hi) / 2, hi):
            y = Y(v)
            body += f'<line x1="{L}" y1="{y:.1f}" x2="{L + pw}" y2="{y:.1f}" stroke="#e5e7eb" stroke-dasharray="3 3"/>'
            body += f'<text x="{L - 6}" y="{y + 4:.1f}" font-size="11" fill="#6b7280" text-anchor="end">{v:,.0f}</text>'
        body += f'<text x="{L - 6}" y="{T - 2}" font-size="10" fill="#9ca3af" text-anchor="end">{_html.escape(unit)}</text>'
    body += (f'<path d="{area}" fill="#bfdbfe" fill-opacity="0.45"/>'
             f'<polyline points="{pts}" fill="none" stroke="#2563eb" stroke-width="{1.5 if compact else 2}" stroke-linejoin="round"/>'
             f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{3 if compact else 4}" fill="#1d4ed8"/>')
    if not compact:
        anchor = "end" if cx > L + pw * 0.7 else "start"
        lx = cx - 8 if anchor == "end" else cx + 8
        body += f'<text x="{lx:.1f}" y="{cy - 8:.1f}" font-size="12" font-weight="600" fill="#1e3a8a" text-anchor="{anchor}">{_html.escape(_fmt_level(vals[-1], unit))}</text>'
    style = "display:block;height:auto;font-family:ui-sans-serif,system-ui,sans-serif"
    return f'<svg viewBox="0 0 {W} {H}" width="100%" preserveAspectRatio="none" aria-hidden="true" style="{style}">{body}</svg>' if compact else \
           f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" style="max-width:100%;{style}">{body}</svg>'


def generate_lake_level_html(water_name, lake_levels):
    """
    Compact water-level card (one-line stats + sparkline) that opens a
    full 12-month chart in a <dialog> when clicked. Returns "" when there
    is no series for this water.
    """
    import html as _html
    from datetime import datetime as _dt, timedelta as _td

    info = lake_levels.get(water_name)
    if not isinstance(info, dict) or not info.get("series"):
        return ""
    unit = info.get("unit", "ft")
    series = [(d, float(v)) for d, v in info["series"] if v is not None]
    if len(series) < 2:
        return ""
    last_date = _dt.strptime(series[-1][0], "%Y-%m-%d")
    start = last_date - _td(days=365)
    series = [(d, v) for d, v in series if _dt.strptime(d, "%Y-%m-%d") >= start]
    if len(series) < 2:
        return ""

    vals = [v for _, v in series]
    cur, hi, lo = vals[-1], max(vals), min(vals)
    hi_date = series[vals.index(hi)][0]
    lo_date = series[vals.index(lo)][0]
    d30 = last_date - _td(days=30)
    before = [v for d, v in series if _dt.strptime(d, "%Y-%m-%d") <= d30]
    change30 = (cur - before[-1]) if before else None

    def dstr(d):
        return _dt.strptime(d, "%Y-%m-%d").strftime("%b %d")

    what = "Storage" if unit == "ac-ft" else "Water level"
    chg = ""
    if change30 is not None:
        sign = "+" if change30 >= 0 else "&minus;"
        color = "text-green-700" if change30 >= 0 else "text-red-700"
        chg = f'<span class="{color}">{sign}{abs(change30):,.1f} {unit} in 30 days</span>'
    src_url = AGENCY_LINKS.get(info.get("source", ""), "")
    src = _html.escape(info.get("label", "") or info.get("source", ""))
    if src_url:
        src = f'<a href="{src_url}" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:underline">{src}</a>'

    spark = _level_svg(series, unit, 240, 48, 2, 2, 4, 4, compact=True)
    full = _level_svg(series, unit, 720, 240, 64, 16, 16, 30, compact=False)
    dlg = "lake-level-dialog"

    parts = [
        # compact card
        f'<button type="button" onclick="document.getElementById(\'{dlg}\').showModal()" '
        'class="mb-6 w-full text-left bg-white border border-gray-200 rounded-lg px-4 py-2 flex items-center gap-4 hover:bg-blue-50 focus:outline-none focus:ring-2 focus:ring-blue-300" '
        f'aria-label="Show 12-month {what.lower()} chart for {_html.escape(water_name)}">',
        '<div class="flex-1 min-w-0 text-sm text-gray-800">',
        f'<span class="font-semibold">{what}:</span> <strong>{_html.escape(_fmt_level(cur, unit))}</strong> '
        f'<span class="text-gray-500">({dstr(series[-1][0])})</span>' + (f' &nbsp;&middot;&nbsp; {chg}' if chg else '') +
        ' <span class="text-xs text-blue-600 whitespace-nowrap">&nbsp;&middot; 12-month chart &#9656;</span>',
        '</div>',
        f'<div class="w-40 sm:w-60 h-12 flex-none">{spark}</div>',
        '</button>',
        # full chart dialog
        f'<dialog id="{dlg}" class="rounded-lg p-0 w-11/12 max-w-3xl shadow-xl" onclick="if(event.target===this)this.close()">',
        '<div class="p-5">',
        '<div class="flex items-start justify-between gap-4 mb-1">',
        f'<h3 class="text-lg font-bold text-gray-800">{_html.escape(water_name)} &mdash; {what.lower()}, last 12 months</h3>',
        f'<button type="button" onclick="document.getElementById(\'{dlg}\').close()" class="text-gray-500 hover:text-gray-800 text-2xl leading-none" aria-label="Close">&times;</button>',
        '</div>',
        '<p class="text-sm text-gray-700 mb-3">'
        f'<strong>{_html.escape(_fmt_level(cur, unit))}</strong> on {dstr(series[-1][0])} &nbsp;&middot;&nbsp; '
        f'12-mo high {_html.escape(_fmt_level(hi, unit))} ({dstr(hi_date)}) &nbsp;&middot;&nbsp; '
        f'low {_html.escape(_fmt_level(lo, unit))} ({dstr(lo_date)})' + (f' &nbsp;&middot;&nbsp; {chg}' if chg else '') + '</p>',
        full,
        f'<p class="text-xs text-gray-500 mt-2">Daily readings from {src}. Elevation is the water surface above sea level; a falling line means ramps and shoreline access get longer. This is not a ramp-status feed &mdash; see park alerts and call ahead before towing.</p>',
        '</div></dialog>',
    ]
    return "\n".join(parts)


def _park_key(name):
    """Normalize a NM State Parks park name for matching against water_authority units."""
    n = re.sub(r'\(.*?\)', ' ', name.lower())
    n = n.replace('&', ' and ').replace('ctr.', 'center')
    n = re.sub(r'\bstate park\b', ' ', n)
    n = re.sub(r'[^a-z0-9 ]', ' ', n)
    return ' '.join(n.split())


def find_park_alerts(water_name, park_alerts, water_authority):
    """Return (park_name, alerts) for the state park that manages this water, or (None, [])."""
    info = water_authority.get(water_name)
    if not isinstance(info, dict) or info.get("category") not in ("state_park", "mixed"):
        return None, []
    unit = _park_key(info.get("unit", "") or "")
    if not unit:
        return None, []
    for park, pdata in park_alerts.items():
        if park.startswith("_"):
            continue
        pk = _park_key(park)
        if pk and (unit.startswith(pk) or pk in unit):
            return park, pdata.get("alerts", [])
    return None, []


def generate_park_alerts_html(water_name, park_alerts, water_authority, fetched=""):
    """
    Current NM State Parks alerts for the park that manages this water.
    Boating/ramp/lake-level alerts are shown inline; everything else is
    collapsed behind a "more park alerts" toggle so the top of the page
    stays short.
    """
    import html as _html
    park, alerts = find_park_alerts(water_name, park_alerts, water_authority)
    if not alerts:
        return ""
    alerts = sorted(alerts, key=lambda a: (not a.get("boating"), a.get("posted", "")))[:10]
    boating = [a for a in alerts if a.get("boating")]
    other = [a for a in alerts if not a.get("boating")]
    phone = (park_alerts.get(park) or {}).get("phone", "")

    def li(a):
        when = a.get("posted", "")
        if a.get("until") and a["until"] != "ongoing" and not a["until"].endswith("/2999"):
            when += f' &ndash; {a["until"]}'
        elif when:
            when += ' &ndash; ongoing'
        return (f'<li class="text-sm text-gray-800">{_html.escape(a.get("text", ""))}'
                + (f' <span class="text-xs text-gray-500 whitespace-nowrap">({when})</span>' if when else '') + '</li>')

    parts = ['<div class="mb-6 border-l-4 border-amber-500 bg-amber-50 px-5 py-3 rounded-r-lg">']
    parts.append(f'<p class="text-sm font-bold text-amber-900 mb-1">Park Alerts &mdash; {_html.escape(park)} State Park</p>')
    if boating:
        parts.append('<ul class="space-y-1">' + "".join(li(a) for a in boating) + '</ul>')
    if other:
        label = f'{len(other)} more park alert{"s" if len(other) != 1 else ""}' if boating else f'{len(other)} park alert{"s" if len(other) != 1 else ""}'
        parts.append(f'<details class="mt-1"><summary class="text-sm text-amber-900 cursor-pointer select-none">{label} (campgrounds, trails, seasonal closures)</summary>')
        parts.append('<ul class="space-y-1 mt-2">' + "".join(li(a) for a in other) + '</ul></details>')
    foot = 'Source: <a href="https://wwwapps.emnrd.nm.gov/SPD/ParksReportingPublicDisplay/Closure" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:underline">NM State Parks alerts</a>'
    if fetched:
        foot += f', checked {_html.escape(fetched[:10])}'
    if phone:
        foot += f'. Park office {_html.escape(phone)}'
    parts.append(f'<p class="text-xs text-gray-500 mt-2">{foot}.</p></div>')
    return "\n".join(parts)


def generate_unstocked_html(water_name, note, booklet_species=None, advisory_url=None):
    """Summary block for a water that gets a page but has no NMDGF stocking records."""
    import html as _html
    parts = ['<div class="mb-6 bg-gradient-to-r from-gray-50 to-blue-50 border-l-4 border-gray-400 p-6 rounded-r-lg">',
             '<div class="inline-block px-4 py-2 rounded-full border text-sm font-semibold mb-3 bg-gray-100 text-gray-700 border-gray-300">Not on the NMDGF stocking schedule</div>']
    if note:
        for para in re.split(r'\n\s*\n', note.strip()):
            parts.append(f'<p class="text-gray-700 mt-1">{_html.escape(para.strip())}</p>')
    if booklet_species:
        parts.append('<div class="mt-4 pt-3 border-t border-gray-200"><p class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Species Present</p><div class="flex flex-wrap gap-2">')
        for sp in booklet_species:
            parts.append(f'<span class="px-3 py-1 bg-green-100 text-green-800 text-xs font-medium rounded-full" title="Present per fishing regulations">{_html.escape(sp)} &#10022;</span>')
        parts.append('</div><p class="text-xs text-gray-400 mt-1">&#10022; Listed in fishing regulations (not stocked)</p>')
        if advisory_url:
            parts.append(f'<p class="text-xs mt-2"><a href="{_html.escape(advisory_url, quote=True)}" target="_blank" rel="noopener noreferrer" class="text-red-600 hover:underline font-medium">Consumption Advisory</a></p>')
        parts.append('</div>')
    elif advisory_url:
        parts.append(f'<p class="text-xs mt-3"><a href="{_html.escape(advisory_url, quote=True)}" target="_blank" rel="noopener noreferrer" class="text-red-600 hover:underline font-medium">Consumption Advisory</a></p>')
    parts.append('</div>')
    return "".join(parts)


BOAT_LABELS = {
    "none": "No boats",
    "non_motorized": "Non-motorized only (kayaks, float tubes, canoes)",
    "electric_only": "Electric motors only",
    "no_wake": "Motors allowed, no-wake",
    "unrestricted": "Motors allowed",
}


def generate_water_notes_html(water_name, water_notes):
    """
    Generate the "Local Notes" block from water_notes.json.

    Only fields with content are rendered; a water with no filled fields
    returns "" so the page has no empty container or filler text. Author
    text is HTML-escaped. `boats` must be one of BOAT_LABELS or the row is
    dropped rather than guessed.
    """
    import html as _html

    note = water_notes.get(water_name)
    if not isinstance(note, dict):
        return ""

    def field(key):
        val = note.get(key)
        return val.strip() if isinstance(val, str) else ""

    rows = []
    if field("access_parking"):
        rows.append(("Access / parking", _html.escape(field("access_parking"))))
    if field("shoreline_ramps"):
        rows.append(("Shoreline / boat ramps", _html.escape(field("shoreline_ramps"))))
    boats = field("boats")
    if boats in BOAT_LABELS:
        text = _html.escape(BOAT_LABELS[boats])
        if field("boats_source"):
            text += f' <span class="text-gray-500 text-sm">(source: {_html.escape(field("boats_source"))})</span>'
        rows.append(("Boats", text))
    elif boats:
        print(f"  [water-notes] {water_name}: unknown boats value '{boats}' ignored.")
    description = field("description")

    if not rows and not description:
        return ""

    parts = []
    parts.append('<div class="mb-6 border-l-4 border-green-600 bg-green-50 p-6 rounded-r-lg">')
    parts.append('<h3 class="text-xl font-bold text-green-900 mb-1 flex items-center">')
    parts.append('<svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">')
    parts.append('<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"></path>')
    parts.append('</svg>')
    parts.append('Local Notes</h3>')
    parts.append('<p class="text-sm text-green-800 mb-4">First-hand observations from the site author, not official NMDGF information. Conditions and rules change; verify before you go.</p>')
    if description:
        for para in re.split(r'\n\s*\n', description):
            parts.append(f'<p class="text-gray-800 mb-3">{_html.escape(para.strip())}</p>')
    if rows:
        parts.append('<dl class="grid grid-cols-1 sm:grid-cols-[max-content_1fr] gap-x-6 gap-y-2 mt-2">')
        for label, value in rows:
            parts.append(f'<dt class="font-semibold text-gray-700">{label}</dt>')
            parts.append(f'<dd class="text-gray-800">{value}</dd>')
        parts.append('</dl>')
    parts.append('</div>')
    return "\n".join(parts)


def generate_regulation_html(water_name, regulations_data):
    """
    Generate HTML for fishing regulations if available for this water body.

    Args:
        water_name: Name of the water body
        regulations_data: Dict of matched regulations

    Returns:
        HTML string or empty string if no regulations
    """
    if water_name not in regulations_data:
        return ""

    reg_info = regulations_data[water_name]
    regulations = reg_info.get("regulations", {})

    if not regulations:
        return ""

    # Build the HTML
    html_parts = []
    html_parts.append('<div class="mb-6 border-l-4 border-blue-500 bg-blue-50 p-6 rounded-r-lg">')
    html_parts.append('<h3 class="text-xl font-bold text-blue-900 mb-4 flex items-center">')
    html_parts.append('<svg class="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">')
    html_parts.append('<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"></path>')
    html_parts.append('</svg>')
    html_parts.append('Special Regulations</h3>')

    def get_designation_badge(designation):
        """Get the appropriate designation image for a chile water designation."""
        designation_lower = designation.lower()

        if 'green' in designation_lower:
            # Green chile: green chile image
            return f'<img src="/public/images/designations/Green_Chile_Water.png" alt="{designation}" class="inline-block h-16 w-auto" title="{designation}">'
        elif 'red' in designation_lower:
            # Red chile: red chile image
            return f'<img src="/public/images/designations/Red_Chile_Water.png" alt="{designation}" class="inline-block h-16 w-auto" title="{designation}">'
        elif 'xmas' in designation_lower or 'x-mas' in designation_lower:
            # Christmas chile: red and green chile image
            return f'<img src="/public/images/designations/Xmas-Chile_Water.png" alt="{designation}" class="inline-block h-16 w-auto" title="{designation}">'
        else:
            # Default: text badge for any other designation types
            return f'<span class="inline-block bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-sm font-semibold">{designation}</span>'

    # Special Trout Water - Lake
    def render_stw(entries):
        entries = entries if isinstance(entries, list) else [entries]
        for idx, stw in enumerate(entries):
            if idx:
                html_parts.append('<hr class="my-4 border-blue-200">')
            designation = stw.get("designation", "")
            if designation:
                html_parts.append(f'<div class="mb-4">{get_designation_badge(designation)}</div>')
            if stw.get("info"):
                html_parts.append(f'<p class="text-gray-700 mb-3"><strong>Reach:</strong> {stw["info"]}</p>')
            if stw.get("tackle_regulation"):
                html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Tackle:</strong> {stw["tackle_regulation"]}</p>')
            if stw.get("pro_regulation"):
                html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Bag Limit:</strong> {stw["pro_regulation"]}</p>')
            if stw.get("trout_present"):
                html_parts.append(f'<p class="text-gray-600 text-sm mt-3"><strong>Species:</strong> {stw["trout_present"]}</p>')

    # Special Trout Water - Lake(s) and Stream reach(es)
    if "special_trout_water_lake" in regulations:
        render_stw(regulations["special_trout_water_lake"])
    if "special_trout_water_stream" in regulations:
        if "special_trout_water_lake" in regulations:
            html_parts.append('<hr class="my-4 border-blue-200">')
        render_stw(regulations["special_trout_water_stream"])

    # Trophy Bass
    if "trophy_bass" in regulations:
        tb = regulations["trophy_bass"]
        html_parts.append('<div class="mt-4 pt-4 border-t border-blue-200">')
        html_parts.append('<p class="text-gray-700 font-semibold mb-2">Trophy Bass Water</p>')
        if tb.get("regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2">{tb["regulation"]}</p>')
        if tb.get("info"):
            html_parts.append(f'<p class="text-gray-600 text-sm">{tb["info"]}</p>')
        html_parts.append('</div>')

    # Summer Catfish
    if "summer_catfish" in regulations:
        sc = regulations["summer_catfish"]
        html_parts.append('<div class="mt-4 pt-4 border-t border-blue-200">')
        html_parts.append('<p class="text-gray-700 font-semibold mb-2">Special Summer Catfish Water</p>')
        if sc.get("regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2">{sc["regulation"]}</p>')
        if sc.get("info"):
            html_parts.append(f'<p class="text-gray-600 text-sm">{sc["info"]}</p>')
        html_parts.append('</div>')

    # Boating restriction from the rules booklet (takes precedence over the
    # generic "motorized craft allowed" wording on the ramp layer)
    boating = regulations.get("boating")
    if isinstance(boating, dict) and boating.get("text"):
        label = BOAT_LABELS.get(boating.get("rule", ""), "")
        html_parts.append('<div class="mt-4 pt-4 border-t border-blue-200">')
        html_parts.append('<p class="text-gray-700 font-semibold mb-2">Boating' + (f' <span class="font-normal text-gray-500">&middot; {html_lib.escape(label)}</span>' if label else '') + '</p>')
        html_parts.append(f'<p class="text-gray-700 mb-1">{html_lib.escape(boating["text"])}</p>')
        if boating.get("source"):
            html_parts.append(f'<p class="text-xs text-gray-500">Source: {html_lib.escape(boating["source"])}</p>')
        html_parts.append('</div>')

    # Boat ramps (NMDGF Fishing Waters Map): ramp type, who runs it, motor rules
    ramps = regulations.get("boat_ramps") or []
    if ramps:
        html_parts.append('<div class="mt-4 pt-4 border-t border-blue-200">')
        html_parts.append(f'<p class="text-gray-700 font-semibold mb-2">Boat Ramps ({len(ramps)})</p>')
        seen = set()
        for ramp in ramps:
            bits = [ramp.get("ramp_type") or "", ramp.get("ownership") or "", ramp.get("info") or ""]
            line = " &middot; ".join(html_lib.escape(b) for b in bits if b)
            if line and line not in seen:
                seen.add(line)
                html_parts.append(f'<p class="text-gray-700 text-sm mb-1">{line}</p>')
        html_parts.append('</div>')

    # Disclaimer
    html_parts.append('<p class="text-xs text-gray-500 mt-4 pt-4 border-t border-blue-200">')
    html_parts.append('<strong>Note:</strong> This information is sourced from NM Game & Fish GIS data. ')
    html_parts.append('Always check the official <a href="https://wildlife.dgf.nm.gov/fishing/" target="_blank" class="text-blue-600 hover:underline">NM Game & Fish fishing regulations</a> for the most current rules.')
    html_parts.append('</p>')

    html_parts.append('</div>')

    return ''.join(html_parts)

def generate_water_image_html(water_name, water_images):
    """
    Returns a narrow banner HTML block if an approved image exists for this water,
    otherwise returns an empty string so the page looks identical to today.
    """
    entry = water_images.get(water_name)
    if not entry or not entry.get("image"):
        return ""
    img = entry["image"]
    url = img.get("url", "")
    attribution = img.get("attribution", "Wikimedia Commons")
    position = img.get("position", "center 50%")
    page_url = img.get("page_url", "")
    if not url:
        return ""
    # Strip any HTML tags from attribution (Wikimedia sometimes returns markup)
    attribution = re.sub(r'<[^>]+>', '', attribution).strip()
    # Escape any double-quotes in attribution for HTML attribute safety
    attribution_safe = attribution.replace('"', '&quot;')
    # CC/Flickr licenses require linking back to the source page. If we have one,
    # render the credit as a link; otherwise plain text (Wikimedia dam photos etc.).
    if page_url:
        page_url_safe = page_url.replace('"', '&quot;')
        credit = (
            f'<a href="{page_url_safe}" target="_blank" rel="noopener noreferrer nofollow" '
            f'style="color:rgba(255,255,255,0.8);text-decoration:underline;">'
            f'{attribution_safe}</a>'
        )
    else:
        credit = attribution_safe
    return (
        f'<div class="mb-6 rounded-lg overflow-hidden shadow" '
        f'style="background: linear-gradient(rgba(10,30,90,0.35), rgba(10,30,90,0.35)), '
        f'url(\'{url}\') {position}/cover no-repeat; height: 160px;" '
        f'role="img" '
        f'aria-label="{water_name}, New Mexico">'
        f'<div style="height:100%;display:flex;align-items:flex-end;padding:8px 12px;">'
        f'<span style="color:rgba(255,255,255,0.7);font-size:0.65rem;">'
        f'{credit}'
        f'</span></div></div>'
    )

def _canonical_water_key(name):
    """Normalize a water-body name for fuzzy matching across data sources.

    Auxiliary data (fishing-rules booklet, ArcGIS regs, consumption advisories)
    names waters slightly differently than the stocking data does -- e.g.
    "Navajo Lake" vs "Navajo Reservoir", "Sumner Lake" vs "Lake Sumner". This
    collapses those differences by lowercasing, dropping parenthetical segment
    qualifiers, treating Lake/Reservoir as equivalent, and ignoring word order.
    """
    s = name.lower()
    s = re.sub(r'\(.*?\)', '', s)                    # drop segment qualifiers e.g. "(Quality)"
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = re.sub(r'\b(reservoir|lake)\b', 'water', s)  # lake == reservoir
    return ' '.join(sorted(t for t in s.split() if t))  # word-order insensitive


def _resolve_by_canonical(source, canonical_names):
    """Re-key a display-name-keyed dict onto canonical stocking water names.

    Exact matches always win. A non-exact source key is attached only when it
    normalizes to exactly ONE canonical water name; keys that are ambiguous
    (normalize to several waters, e.g. a generic "Pecos River" matching six
    river segments) or orphaned (match no stocked water) are left out and
    returned in `unmatched` for reporting.

    Returns (resolved_dict_keyed_by_canonical_name, unmatched_list).
    """
    canonical_set = set(canonical_names)
    canon_by_norm = {}
    for c in canonical_names:
        canon_by_norm.setdefault(_canonical_water_key(c), []).append(c)

    resolved = {}
    # Pass 1: exact matches take priority.
    for key, val in source.items():
        if key in canonical_set:
            resolved[key] = val
    # Pass 2: unambiguous fuzzy matches for keys not already resolved exactly.
    unmatched = []
    for key, val in source.items():
        if key in canonical_set:
            continue
        targets = canon_by_norm.get(_canonical_water_key(key), [])
        if len(targets) == 1 and targets[0] not in resolved:
            resolved[targets[0]] = val
        else:
            unmatched.append((key, targets))
    return resolved, unmatched


def generate_static_pages(data):
    """
    Generates an individual HTML page for each water body.
    Validates NMDGF URLs and falls back to local copies when needed.
    """
    print("\n--- Starting Static Page Generation ---")
    if not os.path.exists(TEMPLATE_FILE):
        print(f"Error: Template file '{TEMPLATE_FILE}' not found. Cannot generate pages.")
        return

    with open(TEMPLATE_FILE, "r", encoding="utf-8") as f:
        template_html = f.read()

    # Waters that get a page without NMDGF stocking records (e.g. Cochiti Lake).
    # Merged here only; the stocking data files are left alone.
    extra_notes = {}
    if os.path.exists(EXTRA_WATERS_FILE):
        try:
            with open(EXTRA_WATERS_FILE, 'r', encoding='utf-8') as f:
                extras = {k: v for k, v in json.load(f).items() if not k.startswith('_')}
            data = dict(data)
            for name, info in extras.items():
                if name in data:
                    continue
                data[name] = {"records": [], "coords": info.get("coords")}
                extra_notes[name] = info.get("stocking_note", "")
            print(f"Loaded {len(extras)} extra (unstocked) waters.")
        except Exception as e:
            print(f"Warning: Could not load {EXTRA_WATERS_FILE}: {e}")

    # Daily reservoir levels and NM State Parks alerts
    lake_levels = {}
    if os.path.exists(LAKE_LEVELS_FILE):
        try:
            with open(LAKE_LEVELS_FILE, 'r', encoding='utf-8') as f:
                lake_levels = {k: v for k, v in json.load(f).items() if not k.startswith('_')}
            print(f"Loaded lake levels for {len(lake_levels)} reservoirs.")
        except Exception as e:
            print(f"Warning: Could not load {LAKE_LEVELS_FILE}: {e}")
    park_alerts, alerts_fetched = {}, ""
    if os.path.exists(PARK_ALERTS_FILE):
        try:
            with open(PARK_ALERTS_FILE, 'r', encoding='utf-8') as f:
                raw_alerts = json.load(f)
            alerts_fetched = raw_alerts.get("_fetched", "")
            park_alerts = {k: v for k, v in raw_alerts.items() if not k.startswith('_')}
            print(f"Loaded park alerts for {len(park_alerts)} state parks.")
        except Exception as e:
            print(f"Warning: Could not load {PARK_ALERTS_FILE}: {e}")

    # Load regulation data if available
    regulations_data = {}
    regulations_file = "matched_regulations.json"
    if os.path.exists(regulations_file):
        try:
            with open(regulations_file, 'r', encoding='utf-8') as f:
                regulations_json = json.load(f)
                regulations_data = regulations_json.get("matched_waters", {})
            print(f"Loaded regulation data for {len(regulations_data)} water bodies.")
        except Exception as e:
            print(f"Warning: Could not load regulation data: {e}")

    # Load booklet species data from NMDGF fishing rules PDF
    water_species_data = {}
    if os.path.exists("water_species.json"):
        try:
            with open("water_species.json", 'r', encoding='utf-8') as f:
                raw = json.load(f)
                # Strip metadata keys starting with underscore
                water_species_data = {k: v for k, v in raw.items() if not k.startswith('_')}
            print(f"Loaded booklet species data for {len(water_species_data)} water bodies.")
        except Exception as e:
            print(f"Warning: Could not load water_species.json: {e}")

    # Load water images
    water_images = {}
    if os.path.exists(WATER_IMAGES_FILE):
        try:
            with open(WATER_IMAGES_FILE, 'r', encoding='utf-8') as f:
                water_images = json.load(f)
            img_count = sum(1 for v in water_images.values() if v.get("image"))
            print(f"Loaded water images for {img_count} water bodies.")
        except Exception as e:
            print(f"Warning: Could not load water_images.json: {e}")

    # Load consumption advisory page numbers
    consumption_advisories = {}
    if os.path.exists("consumption_advisories.json"):
        try:
            with open("consumption_advisories.json", 'r', encoding='utf-8') as f:
                raw = json.load(f)
                advisory_pdf_url = raw.get("_pdf_url", "")
                consumption_advisories = {k: v for k, v in raw.items() if not k.startswith('_')}
            print(f"Loaded consumption advisory data for {len(consumption_advisories)} water bodies.")
        except Exception as e:
            print(f"Warning: Could not load consumption_advisories.json: {e}")

    # Load hand-written water notes (author prose; never generated)
    water_notes = {}
    if os.path.exists(WATER_NOTES_FILE):
        try:
            with open(WATER_NOTES_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
                water_notes = {k: v for k, v in raw.items() if not k.startswith('_')}
            filled = sum(1 for v in water_notes.values() if isinstance(v, dict) and any((x or "").strip() for x in v.values() if isinstance(x, str)))
            print(f"Loaded water notes: {filled} of {len(water_notes)} entries have content.")
        except Exception as e:
            print(f"Warning: Could not load {WATER_NOTES_FILE}: {e}")

    # Load managing-authority data (hand-maintained; never generated)
    water_authority = {}
    if os.path.exists(WATER_AUTHORITY_FILE):
        try:
            with open(WATER_AUTHORITY_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
                water_authority = {k: v for k, v in raw.items() if not k.startswith('_')}
            renderable = sum(1 for v in water_authority.values() if isinstance(v, dict) and v.get("confidence") in ("high", "medium"))
            print(f"Loaded water authority: {renderable} of {len(water_authority)} entries at high/medium confidence.")
        except Exception as e:
            print(f"Warning: Could not load {WATER_AUTHORITY_FILE}: {e}")

    # Re-key auxiliary data sources onto canonical stocking water names so that
    # naming differences (Navajo Lake -> Navajo Reservoir, Sumner Lake -> Lake
    # Sumner, etc.) don't cause silent lookup misses. Ambiguous/orphan keys are
    # reported so mismatches surface on every build instead of failing quietly.
    canonical_names = list(data.keys())
    water_species_data, booklet_unmatched = _resolve_by_canonical(water_species_data, canonical_names)
    regulations_data, regs_unmatched = _resolve_by_canonical(regulations_data, canonical_names)
    consumption_advisories, advisory_unmatched = _resolve_by_canonical(consumption_advisories, canonical_names)
    water_notes, notes_unmatched = _resolve_by_canonical(water_notes, canonical_names)
    water_authority, authority_unmatched = _resolve_by_canonical(water_authority, canonical_names)

    # Boating rules from the booklet: merged into the regulations block so they
    # render next to the boat ramps. A water with a boat rule but no other
    # regulation gets a regulations entry containing only "boating".
    boat_rules, boat_source_default = {}, ""
    if os.path.exists(BOAT_RULES_FILE):
        try:
            with open(BOAT_RULES_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
                boat_source_default = raw.get("_source_default", "")
                boat_rules = {k: v for k, v in raw.items() if not k.startswith('_')}
            print(f"Loaded boat rules for {len(boat_rules)} water bodies.")
        except Exception as e:
            print(f"Warning: Could not load {BOAT_RULES_FILE}: {e}")
    boat_rules, boats_unmatched = _resolve_by_canonical(boat_rules, canonical_names)
    lake_levels, levels_unmatched = _resolve_by_canonical(lake_levels, canonical_names)
    for name, rule in boat_rules.items():
        if not isinstance(rule, dict) or not (rule.get("text") or "").strip():
            continue
        entry = regulations_data.setdefault(name, {"regulations": {}})
        entry.setdefault("regulations", {})["boating"] = {
            "rule": rule.get("rule", ""),
            "text": rule["text"].strip(),
            "source": (rule.get("source") or "").strip() or boat_source_default,
        }
    for src_label, unmatched in (
        ("water_species.json", booklet_unmatched),
        ("matched_regulations.json", regs_unmatched),
        ("consumption_advisories.json", advisory_unmatched),
        (WATER_NOTES_FILE, notes_unmatched),
        (WATER_AUTHORITY_FILE, authority_unmatched),
        (BOAT_RULES_FILE, boats_unmatched),
        (LAKE_LEVELS_FILE, levels_unmatched),
    ):
        for key, targets in unmatched:
            reason = f"ambiguous -> {targets}" if targets else "no matching stocked water"
            print(f"  [species-match] {src_label}: '{key}' not attached ({reason}).")

    # Cache for URL validation to avoid checking same URL multiple times
    url_validation_cache = {}
    validated_count = 0
    fallback_count = 0

    generated_count = 0
    for water_name, water_data in data.items():
        print(f"  -> Generating page for {water_name}...")
        filename = re.sub(r'[^a-z0-9]+', '-', water_name.lower()).strip('-') + ".html"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # Generate summary statistics
        records = water_data.get("records", [])
        coords = water_data.get("coords")
        summary_stats = generate_summary_stats(records)

        # Pull native/wild species from regulation data if available
        reg_species = None
        if water_name in regulations_data:
            for reg_block in regulations_data[water_name].get('regulations', {}).values():
                # STW entries are lists of reaches; ramps are lists too.
                blocks = reg_block if isinstance(reg_block, list) else [reg_block]
                for b in blocks:
                    trout_present = b.get('trout_present', '') if isinstance(b, dict) else ''
                    if trout_present:
                        reg_species = trout_present
                        break
                if reg_species:
                    break

        # Pull species from NMDGF fishing rules booklet
        booklet_species = water_species_data.get(water_name, [])

        # Pull consumption advisory page number if applicable
        advisory_page = consumption_advisories.get(water_name)
        advisory_url = f"{advisory_pdf_url}#page={advisory_page}" if advisory_page and advisory_pdf_url else None

        if water_name in extra_notes:
            summary_html = generate_unstocked_html(water_name, extra_notes[water_name], booklet_species=booklet_species, advisory_url=advisory_url)
        else:
            summary_html = generate_summary_html(water_name, summary_stats, reg_species=reg_species, booklet_species=booklet_species, advisory_url=advisory_url)
        meta_description = generate_meta_description(water_name, summary_stats)

        table_rows_html = ""
        for record in records:
            date_obj = datetime.strptime(record['date'], "%Y-%m-%d")
            display_date = date_obj.strftime("%b %d, %Y")

            report_link_html = ""
            if record.get("reportUrl"):
                url = record['reportUrl']

                # If it's an NMDGF URL, validate it and potentially fall back to local
                if 'wildlife.dgf.nm.gov' in url:
                    # Check cache first
                    if url not in url_validation_cache:
                        url_validation_cache[url] = validate_url(url)

                    if url_validation_cache[url]:
                        validated_count += 1
                    else:
                        fallback = get_fallback_url(url)
                        if fallback:
                            url = fallback
                            fallback_count += 1

                # rel="nofollow" so Google doesn't pass authority to the NMDGF PDF.
                # Hidden anchor — gives Google a proper tag to read, no visible UI change.
                report_link_html = f'<a href="{url}" target="_blank" rel="nofollow noopener noreferrer" style="display:none" onclick="event.stopPropagation()"></a>'

            table_rows_html += f"""
                <tr class="clickable-row hover:bg-gray-50" onclick="this.querySelector('a[target]')?.click()">
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{display_date}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-800">{record['species']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['quantity']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['length']}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{record['hatchery']}{report_link_html}</td>
                </tr>
            """

        if not records and water_name in extra_notes:
            table_rows_html = """
                <tr><td colspan="5" class="px-6 py-4 text-sm text-gray-500">No NMDGF stocking records for this water.</td></tr>
            """

        # Generate regulation HTML if available
        regulation_html = generate_regulation_html(water_name, regulations_data)

        # Generate page URL for social media tags
        page_url = f"https://stockingreport.com/public/waters/{filename}"
        schema_org = generate_schema_org(water_name, summary_stats, coords, page_url)

        water_image_html = generate_water_image_html(water_name, water_images)
        water_notes_html = generate_water_notes_html(water_name, water_notes)
        water_authority_html = generate_water_authority_html(water_name, water_authority)
        lake_level_html = generate_lake_level_html(water_name, lake_levels)
        park_alerts_html = generate_park_alerts_html(water_name, park_alerts, water_authority, alerts_fetched)

        page_html = template_html.replace("{{WATER_NAME}}", water_name)
        if water_notes_html:
            page_html = page_html.replace("{{WATER_NOTES}}", water_notes_html)
        else:
            # Drop the whole placeholder line so pages without notes are unchanged.
            page_html = re.sub(r'[ \t]*\{\{WATER_NOTES\}\}\r?\n', '', page_html)
        if water_authority_html:
            page_html = page_html.replace("{{AUTHORITY}}", water_authority_html)
        else:
            page_html = re.sub(r'[ \t]*\{\{AUTHORITY\}\}\r?\n', '', page_html)
        if lake_level_html:
            page_html = page_html.replace("{{LAKE_LEVEL}}", lake_level_html)
        else:
            page_html = re.sub(r'[ \t]*\{\{LAKE_LEVEL\}\}\r?\n', '', page_html)
        if park_alerts_html:
            page_html = page_html.replace("{{PARK_ALERTS}}", park_alerts_html)
        else:
            page_html = re.sub(r'[ \t]*\{\{PARK_ALERTS\}\}\r?\n', '', page_html)
        page_html = page_html.replace("{{TABLE_ROWS}}", table_rows_html)
        page_html = page_html.replace("{{SUMMARY}}", summary_html)
        page_html = page_html.replace("{{REGULATIONS}}", regulation_html)
        page_html = page_html.replace("{{WATER_IMAGE}}", water_image_html)
        page_html = page_html.replace("{{META_DESCRIPTION}}", meta_description)
        page_html = page_html.replace("{{PAGE_URL}}", page_url)
        page_html = page_html.replace("{{SCHEMA_ORG}}", schema_org)

        with open(filepath, "w", encoding="utf-8") as f:
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
