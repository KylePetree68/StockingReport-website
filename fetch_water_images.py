"""
fetch_water_images.py

Searches Wikimedia Commons for photos of each NM stocking water.
Outputs water_images.json for review before use in the site.

Strategy per water:
  1. Name search: "Alto Lake New Mexico" via Wikimedia API
  2. If no confident result, geo search by lat/lon (5km radius)
  3. Score candidates by orientation, resolution, and keyword relevance
  4. Save best result with confidence level: high / medium / low / none

Run:  python fetch_water_images.py
Review: water_images.json
"""

import json
import time
import requests

STOCKING_DATA   = "stocking_data_clean.json"
MANUAL_COORDS   = "manual_coordinates.json"
OUTPUT_FILE     = "water_images.json"
REVIEWED_FILE   = "water_images_manual.json"   # human overrides, read-only here

WIKIMEDIA_API   = "https://commons.wikimedia.org/w/api.php"
VALID_THUMB_WIDTHS = [960, 1280]                # Wikimedia-approved sizes
TARGET_WIDTH    = 1280
GEO_RADIUS_M    = 5000                          # 5 km radius for geo search

# Keywords that suggest a photo is relevant to water/fishing
POSITIVE_KEYWORDS = [
    "lake", "reservoir", "river", "creek", "pond",
    "new mexico", "fishing", "recreation", "state park", "scenic", "landscape"
]
# Keywords that strongly suggest a non-photo or wrong subject
NEGATIVE_KEYWORDS = [
    "map", "diagram", "chart", "logo", "icon", "flag", "coat of arms",
    "portrait", "building", "street", "road", "satellite", "topograph",
    "report", "warden", "history", "document", "book", "journal", "magazine",
    "illustration", "drawing", "painting", "sketch", "engraving",
    "census", "survey", "circa", "1800s", "1900s", "1910", "1920",
    "bird", "mammal", "reptile", "plant", "flower", "insect",
    "church", "bridge", "memorial", "hospital", "school",
    "oklahoma", "texas", "arizona", "colorado", "utah", "california"
]

session = requests.Session()
session.headers.update({
    "User-Agent": "NMStockingReport/1.0 (https://stockingreport.com; stocking@kissmygrits.net)"
})


def wikimedia_search(query, limit=5):
    """Text search Wikimedia Commons for images matching query."""
    params = {
        "action":      "query",
        "generator":   "search",
        "gsrnamespace": 6,          # File: namespace
        "gsrsearch":   f"filetype:bitmap {query}",
        "gsrlimit":    limit,
        "prop":        "imageinfo",
        "iiprop":      "url|size|mime|extmetadata",
        "iiurlwidth":  TARGET_WIDTH,
        "format":      "json"
    }
    try:
        r = session.get(WIKIMEDIA_API, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("query", {}).get("pages", {}).values()
    except Exception as e:
        print(f"  Search error ({query}): {e}")
        return []


def wikimedia_geo_search(lat, lon, radius_m=GEO_RADIUS_M, limit=8):
    """Search Wikimedia Commons for images near a coordinate."""
    params = {
        "action":    "query",
        "list":      "geosearch",
        "gscoord":   f"{lat}|{lon}",
        "gsradius":  radius_m,
        "gslimit":   limit,
        "gsnamespace": 6,
        "format":    "json"
    }
    try:
        r = session.get(WIKIMEDIA_API, params=params, timeout=10)
        r.raise_for_status()
        titles = [p["title"] for p in r.json().get("query", {}).get("geosearch", [])]
        if not titles:
            return []
        # Now fetch imageinfo for those titles
        params2 = {
            "action":  "query",
            "titles":  "|".join(titles),
            "prop":    "imageinfo",
            "iiprop":  "url|size|mime|extmetadata",
            "iiurlwidth": TARGET_WIDTH,
            "format":  "json"
        }
        r2 = session.get(WIKIMEDIA_API, params=params2, timeout=10)
        r2.raise_for_status()
        return r2.json().get("query", {}).get("pages", {}).values()
    except Exception as e:
        print(f"  Geo search error ({lat},{lon}): {e}")
        return []


def score_candidate(page, water_name):
    """
    Score a Wikimedia page as a background image candidate.
    Returns (score, image_info_dict) or (0, None) if unusable.
    """
    ii_list = page.get("imageinfo", [])
    if not ii_list:
        return 0, None
    ii = ii_list[0]

    mime = ii.get("mime", "")
    # Only accept JPEG and PNG — no TIFFs (scanned docs), no SVG
    if mime not in ("image/jpeg", "image/png"):
        return 0, None

    width  = ii.get("width", 0)
    height = ii.get("height", 0)
    if width < 800 or height < 400:
        return 0, None                          # too small

    # Must be landscape
    if height > width:
        return 0, None

    # Build the correctly-sized thumb URL
    thumb_url = ii.get("thumburl", "")
    if not thumb_url:
        return 0, None

    # Ensure thumb width is a valid Wikimedia size
    # The API returns iiurlwidth-sized thumb if available
    # Verify it looks right
    if str(TARGET_WIDTH) not in thumb_url and str(VALID_THUMB_WIDTHS[0]) not in thumb_url:
        # Try constructing 960px fallback
        thumb_url = thumb_url.replace(f"{width}px-", "960px-")

    title_lower  = page.get("title", "").lower()
    meta         = ii.get("extmetadata", {})
    description  = (meta.get("ImageDescription", {}).get("value", "") or "").lower()
    artist       = (meta.get("Artist", {}).get("value", "") or "")
    license_val  = (meta.get("LicenseShortName", {}).get("value", "") or "")

    # Check license — prefer CC or public domain, skip Rights Reserved
    license_lower = license_val.lower()
    if "reserved" in license_lower or "no free" in license_lower:
        return 0, None

    combined_text = title_lower + " " + description

    score = 0

    # Must contain at least one water-body keyword to be considered at all
    water_body_terms = ["lake", "reservoir", "river", "creek", "pond", "water",
                        "fishing", "scenic", "landscape", "recreation"]
    if not any(t in combined_text for t in water_body_terms):
        return 0, None

    # Negative keyword hits — disqualify outright if strong match
    neg_hits = sum(1 for kw in NEGATIVE_KEYWORDS if kw in combined_text)
    if neg_hits >= 2:
        return 0, None
    score -= neg_hits * 15

    # Water name words in title/description (title carries more weight)
    name_words = water_name.lower().replace("(", "").replace(")", "").split()
    title_lower_only = page.get("title", "").lower()
    for w in name_words:
        if len(w) <= 3:
            continue
        if w in title_lower_only:
            score += 20         # name word in file title = strong signal
        elif w in description:
            score += 8

    # Positive keyword hits
    score += sum(3 for kw in POSITIVE_KEYWORDS if kw in combined_text)

    # Resolution bonus
    megapixels = (width * height) / 1_000_000
    score += min(int(megapixels), 5)

    # Aspect ratio bonus — wider is better for banners
    ratio = width / height
    if ratio >= 1.5:
        score += 3
    if ratio >= 2.0:
        score += 2

    if score <= 0:
        return 0, None

    # Build attribution string
    artist_clean = artist.replace("<[^>]+>", "").strip() if artist else "Unknown"
    attribution = f"{artist_clean} / Wikimedia Commons ({license_val})"

    return score, {
        "url":         thumb_url,
        "page_title":  page.get("title", ""),
        "attribution": attribution,
        "width":       width,
        "height":      height,
        "score":       score,
    }


def confidence_label(score):
    if score >= 20:
        return "high"
    if score >= 10:
        return "medium"
    if score > 0:
        return "low"
    return "none"


def find_image_for_water(name, coords):
    best_score = 0
    best_info  = None

    # --- Strategy 1: geo search first (most geographically precise) ---
    if coords:
        lat, lon = coords["lat"], coords["lon"]
        for page in wikimedia_geo_search(lat, lon):
            score, info = score_candidate(page, name)
            if score > best_score:
                best_score, best_info = score, info
        time.sleep(0.3)

    # --- Strategy 2: name search (catches waters without nearby tagged photos) ---
    if best_score < 20:
        for query in [
            f'"{name}" New Mexico',
            f'{name} New Mexico',
        ]:
            for page in wikimedia_search(query, limit=6):
                score, info = score_candidate(page, name)
                if score > best_score:
                    best_score, best_info = score, info
            if best_score >= 20:
                break
            time.sleep(0.3)

    return best_score, best_info


def main():
    with open(STOCKING_DATA) as f:
        stocking = json.load(f)
    with open(MANUAL_COORDS) as f:
        manual_coords = json.load(f)

    # Load any existing results so we can resume interrupted runs
    try:
        with open(OUTPUT_FILE) as f:
            results = json.load(f)
        print(f"Resuming — {len(results)} already done.")
    except FileNotFoundError:
        results = {}

    waters = sorted(stocking.keys())
    total  = len(waters)

    for i, name in enumerate(waters, 1):
        if name in results:
            print(f"[{i}/{total}] SKIP (already done): {name}")
            continue

        # Get coords — prefer stocking data, fall back to manual
        water_data = stocking.get(name) or {}
        coords = water_data.get("coords") if water_data else None
        if not coords or not coords.get("lat"):
            coords = manual_coords.get(name)

        print(f"[{i}/{total}] Searching: {name} ...", end=" ", flush=True)
        score, info = find_image_for_water(name, coords)
        confidence  = confidence_label(score)

        results[name] = {
            "confidence": confidence,
            "score":      score,
            "image":      info,
        }

        print(f"{confidence} (score={score})" + (f" -- {info['page_title']}" if info else " -- none"))

        # Save after every water so progress isn't lost
        with open(OUTPUT_FILE, "w") as f:
            json.dump(results, f, indent=2)

        time.sleep(0.5)   # be polite to Wikimedia

    # Summary
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for v in results.values():
        counts[v["confidence"]] += 1

    print()
    print("=== Done ===")
    print(f"  High confidence:   {counts['high']}")
    print(f"  Medium confidence: {counts['medium']}")
    print(f"  Low confidence:    {counts['low']}")
    print(f"  No image found:    {counts['none']}")
    print(f"Results saved to {OUTPUT_FILE}")
    print(f"Review that file, then delete unwanted entries or set image to null.")
    print(f"You can also add manual overrides to {REVIEWED_FILE}.")


if __name__ == "__main__":
    main()
