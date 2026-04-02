import requests
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
    if "special_trout_water_lake" in regulations:
        stw = regulations["special_trout_water_lake"]
        designation = stw.get("designation", "")

        if designation:
            html_parts.append(f'<div class="mb-4">{get_designation_badge(designation)}</div>')

        if stw.get("info"):
            html_parts.append(f'<p class="text-gray-700 mb-3"><strong>Info:</strong> {stw["info"]}</p>')

        if stw.get("tackle_regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Tackle:</strong> {stw["tackle_regulation"]}</p>')

        if stw.get("pro_regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Bag Limit:</strong> {stw["pro_regulation"]}</p>')

        if stw.get("trout_present"):
            html_parts.append(f'<p class="text-gray-600 text-sm mt-3"><strong>Species:</strong> {stw["trout_present"]}</p>')

    # Special Trout Water - Stream
    if "special_trout_water_stream" in regulations:
        stw = regulations["special_trout_water_stream"]
        designation = stw.get("designation", "")

        if designation:
            html_parts.append(f'<div class="mb-4">{get_designation_badge(designation)}</div>')

        if stw.get("info"):
            html_parts.append(f'<p class="text-gray-700 mb-3"><strong>Info:</strong> {stw["info"]}</p>')

        if stw.get("tackle_regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Tackle:</strong> {stw["tackle_regulation"]}</p>')

        if stw.get("pro_regulation"):
            html_parts.append(f'<p class="text-gray-700 mb-2"><strong>Bag Limit:</strong> {stw["pro_regulation"]}</p>')

        if stw.get("trout_present"):
            html_parts.append(f'<p class="text-gray-600 text-sm mt-3"><strong>Species:</strong> {stw["trout_present"]}</p>')

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

    # Disclaimer
    html_parts.append('<p class="text-xs text-gray-500 mt-4 pt-4 border-t border-blue-200">')
    html_parts.append('<strong>Note:</strong> This information is sourced from NM Game & Fish GIS data. ')
    html_parts.append('Always check the official <a href="https://wildlife.dgf.nm.gov/fishing/" target="_blank" class="text-blue-600 hover:underline">NM Game & Fish fishing regulations</a> for the most current rules.')
    html_parts.append('</p>')

    html_parts.append('</div>')

    return ''.join(html_parts)

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
                trout_present = reg_block.get('trout_present', '')
                if trout_present:
                    reg_species = trout_present
                    break

        # Pull species from NMDGF fishing rules booklet
        booklet_species = water_species_data.get(water_name, [])

        # Pull consumption advisory page number if applicable
        advisory_page = consumption_advisories.get(water_name)
        advisory_url = f"{advisory_pdf_url}#page={advisory_page}" if advisory_page and advisory_pdf_url else None

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

        # Generate regulation HTML if available
        regulation_html = generate_regulation_html(water_name, regulations_data)

        # Generate page URL for social media tags
        page_url = f"https://stockingreport.com/public/waters/{filename}"
        schema_org = generate_schema_org(water_name, summary_stats, coords, page_url)

        page_html = template_html.replace("{{WATER_NAME}}", water_name)
        page_html = page_html.replace("{{TABLE_ROWS}}", table_rows_html)
        page_html = page_html.replace("{{SUMMARY}}", summary_html)
        page_html = page_html.replace("{{REGULATIONS}}", regulation_html)
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
