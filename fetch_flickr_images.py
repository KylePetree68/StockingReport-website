"""
fetch_flickr_images.py

Searches Flickr for photos of NM stocking waters that are licensed for
COMMERCIAL use (this site runs ads + affiliate links) AND allow derivatives
(we crop + overlay a gradient on the banner).

Allowed Flickr license IDs:
    4  = CC BY 2.0
    5  = CC BY-SA 2.0
    7  = No known copyright restrictions
    8  = United States Government Work
    9  = Public Domain Dedication (CC0)
    10 = Public Domain Mark
Excluded: all NC (1,2,3) and ND (6, no-derivatives) and All Rights Reserved (0).

Strategy per water (mirrors fetch_water_images.py):
    1. Geo search by lat/lon (photos physically tagged near the water)
    2. Text search "<Water> New Mexico"
    3. Score candidates; keep the top few for HUMAN REVIEW

Output: flickr_candidates.json  (a REVIEW file — NOT water_images.json).
Review it visually, then promote chosen entries into water_images.json.
Nothing here touches water_images.json automatically — same review-before-ship
workflow as the Wikimedia fetch.

API KEY (free, 2 minutes):
    1. Sign in at https://www.flickr.com/services/apps/create/apply/
       (choose "non-commercial" API key for the key itself — this is about the
        API, not the photos; the photo licenses are what govern commercial use)
    2. Copy the "Key" string.
    3. Provide it either way:
         setx FLICKR_API_KEY "your_key_here"      (PowerShell, then reopen shell)
       or drop it in a file named  flickr_api_key.txt  (gitignored) next to this script.

Run:  python fetch_flickr_images.py
      python fetch_flickr_images.py --all      (re-check waters that already have an image too)
Review: flickr_candidates.json
"""

import json
import os
import re
import sys
import time
import requests

STOCKING_DATA   = "stocking_data.json"
MANUAL_COORDS   = "manual_coordinates.json"
EXISTING_IMAGES = "water_images.json"      # read-only: to skip waters already covered
OUTPUT_FILE     = "flickr_candidates.json"
KEY_FILE        = "flickr_api_key.txt"

FLICKR_ENDPOINT = "https://api.flickr.com/services/rest/"
ALLOWED_LICENSES = "4,5,7,8,9,10"
LICENSE_NAMES = {
    "4": "CC BY 2.0", "5": "CC BY-SA 2.0", "7": "No known copyright restrictions",
    "8": "United States Government Work", "9": "CC0 Public Domain", "10": "Public Domain Mark",
}
GEO_RADIUS_KM   = 6          # Flickr max is 32; keep tight so photos are actually AT the water
CANDIDATES_PER_WATER = 4     # how many to keep for review

# Image sizes to prefer (largest first). Each has matching width_/height_ extras.
SIZE_KEYS = [("url_k", "width_k", "height_k"),
             ("url_h", "width_h", "height_h"),
             ("url_l", "width_l", "height_l"),
             ("url_c", "width_c", "height_c")]

POSITIVE_KEYWORDS = ["lake", "reservoir", "river", "creek", "pond", "water",
                     "fishing", "shore", "shoreline", "boat", "scenic", "landscape",
                     "sunset", "sunrise", "state park"]
# Lessons from the Wikimedia pass: botanical / macro / wildlife survey shots score
# high on name match but show no water. Penalize hard.
NEGATIVE_KEYWORDS = ["flower", "wildflower", "plant", "botany", "botanical", "macro",
                     "insect", "bug", "beetle", "spider", "bird", "moth", "butterfly",
                     "fungus", "mushroom", "lichen", "leaf", "cactus", "closeup",
                     "close-up", "portrait", "map", "sign", "building", "museum"]
# Title/tag phrasings that mean "taken NEAR the water", not "of the water"
NEAR_PREFIXES = ["near ", "northeast of", "northwest of", "southeast of",
                 "southwest of", "north of", "south of", "east of", "west of"]

session = requests.Session()
session.headers.update({
    "User-Agent": "NMStockingReport/1.0 (https://stockingreport.com; stocking@kissmygrits.net)"
})


def load_api_key():
    key = os.environ.get("FLICKR_API_KEY", "").strip()
    if not key and os.path.exists(KEY_FILE):
        with open(KEY_FILE) as f:
            key = f.read().strip()
    if not key:
        sys.exit(
            "No Flickr API key found.\n"
            "  Set FLICKR_API_KEY env var, or put the key in flickr_api_key.txt.\n"
            "  Get a free key: https://www.flickr.com/services/apps/create/apply/"
        )
    return key


def flickr_search(api_key, text=None, lat=None, lon=None, per_page=30):
    params = {
        "method":        "flickr.photos.search",
        "api_key":       api_key,
        "license":       ALLOWED_LICENSES,
        "content_type":  1,          # photos only
        "media":         "photos",
        "safe_search":   1,
        "sort":          "relevance",
        "per_page":      per_page,
        "extras":        "license,owner_name,path_alias,geo,description,tags,"
                         + ",".join(k for trio in SIZE_KEYS for k in trio),
        "format":        "json",
        "nojsoncallback": 1,
    }
    if text:
        params["text"] = text
    if lat is not None and lon is not None:
        params["lat"] = lat
        params["lon"] = lon
        params["radius"] = GEO_RADIUS_KM
        params["radius_units"] = "km"
    try:
        r = session.get(FLICKR_ENDPOINT, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("stat") != "ok":
            print(f"  API error: {data.get('message', data.get('stat'))}")
            return []
        return data.get("photos", {}).get("photo", [])
    except Exception as e:
        print(f"  Search error: {e}")
        return []


def best_image(photo):
    """Return (url, width, height) for the largest available acceptable size."""
    for url_k, w_k, h_k in SIZE_KEYS:
        if photo.get(url_k):
            try:
                return photo[url_k], int(photo.get(w_k, 0)), int(photo.get(h_k, 0))
            except (TypeError, ValueError):
                continue
    return None, 0, 0


def score_candidate(photo, water_name, from_geo):
    url, w, h = best_image(photo)
    if not url or w < 800 or h < 400:
        return 0, None
    if h > w:                                   # want landscape for a wide banner
        return 0, None

    title = (photo.get("title", "") or "").lower()
    tags  = (photo.get("tags", "") or "").lower()
    desc  = (photo.get("description", {}) or {}).get("_content", "").lower()
    blob  = f"{title} {tags} {desc}"

    # Hard filters
    if any(neg in blob for neg in NEGATIVE_KEYWORDS):
        return 0, None
    if any(title.startswith(p) or p in title for p in NEAR_PREFIXES):
        return 0, None
    if not any(k in blob for k in POSITIVE_KEYWORDS):
        return 0, None

    score = 0
    if from_geo:
        score += 12                             # physically tagged at the water

    name_words = re.sub(r"[()]", "", water_name.lower()).split()
    for word in name_words:
        if len(word) <= 3:
            continue
        if word in title:
            score += 10
        elif word in tags or word in desc:
            score += 5

    score += sum(2 for k in POSITIVE_KEYWORDS if k in blob)

    megapixels = (w * h) / 1_000_000
    score += min(int(megapixels), 5)
    ratio = w / h
    if ratio >= 1.5:
        score += 3
    if ratio >= 2.0:
        score += 2

    lic = str(photo.get("license", ""))
    owner = photo.get("ownername") or photo.get("owner") or "Unknown"
    alias = photo.get("pathalias") or photo.get("owner")
    page_url = f"https://www.flickr.com/photos/{alias}/{photo.get('id')}"

    info = {
        "url":          url,
        "title":        photo.get("title", ""),
        "page_url":     page_url,               # Flickr ToS: link back to this
        "attribution":  f"{owner} / Flickr ({LICENSE_NAMES.get(lic, 'CC')})",
        "license":      LICENSE_NAMES.get(lic, lic),
        "width":        w,
        "height":       h,
        "from_geo":     from_geo,
        "score":        score,
    }
    return score, info


def find_candidates(api_key, name, coords):
    scored = {}   # id -> (score, info)  dedupe by photo

    def collect(photos, from_geo):
        for p in photos:
            s, info = score_candidate(p, name, from_geo)
            if info and (p["id"] not in scored or s > scored[p["id"]][0]):
                scored[p["id"]] = (s, info)

    if coords and coords.get("lat"):
        collect(flickr_search(api_key, lat=coords["lat"], lon=coords["lon"]), from_geo=True)
        time.sleep(0.4)

    collect(flickr_search(api_key, text=f'{name} New Mexico'), from_geo=False)
    time.sleep(0.4)

    ranked = sorted(scored.values(), key=lambda t: t[0], reverse=True)
    return [info for _, info in ranked[:CANDIDATES_PER_WATER] if info["score"] > 0]


def main():
    api_key = load_api_key()
    recheck_all = "--all" in sys.argv

    with open(STOCKING_DATA, encoding="utf-8") as f:
        stocking = json.load(f)
    with open(MANUAL_COORDS) as f:
        manual = json.load(f)
    try:
        with open(EXISTING_IMAGES, encoding="utf-8") as f:
            existing = json.load(f)
    except FileNotFoundError:
        existing = {}

    try:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            results = json.load(f)
        print(f"Resuming — {len(results)} waters already searched.")
    except FileNotFoundError:
        results = {}

    waters = sorted(stocking.keys())
    total = len(waters)
    for i, name in enumerate(waters, 1):
        if name in results:
            print(f"[{i}/{total}] SKIP (already searched): {name}")
            continue
        if not recheck_all and existing.get(name, {}).get("image"):
            print(f"[{i}/{total}] SKIP (already has image): {name}")
            continue

        wd = stocking.get(name) or {}
        coords = wd.get("coords") if isinstance(wd, dict) else None
        if not coords or not coords.get("lat"):
            coords = manual.get(name)

        print(f"[{i}/{total}] Flickr: {name} ...", end=" ", flush=True)
        cands = find_candidates(api_key, name, coords)
        results[name] = {"candidates": cands}
        if cands:
            print(f"{len(cands)} candidate(s), top score {cands[0]['score']}")
        else:
            print("none")

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        time.sleep(0.5)

    with_cands = sum(1 for v in results.values() if v["candidates"])
    print()
    print("=== Done ===")
    print(f"  Waters searched:        {len(results)}")
    print(f"  With >=1 candidate:     {with_cands}")
    print(f"Review {OUTPUT_FILE} (visually check each image before promoting).")


if __name__ == "__main__":
    main()
