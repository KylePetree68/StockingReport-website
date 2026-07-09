"""
fetch_openverse_images.py

Searches Openverse (https://openverse.org) for photos of NM stocking waters.
Openverse indexes Flickr's openly-licensed photos plus Wikimedia and others,
behind a FREE, keyless API with proper commercial-license filtering — which is
why we use it instead of Flickr's own API (Flickr now requires paid Pro for a key).

License handling: we request license_type=commercial,modification, so every
result already permits commercial use (site runs ads/affiliate) AND derivatives
(we crop + overlay a gradient). That yields CC0 / PD / CC-BY / CC-BY-SA and
excludes all NC and ND licenses automatically.

Per water: one text query "<Water> New Mexico" (parentheticals stripped),
score the candidates, keep the top few for HUMAN REVIEW.

Output: openverse_candidates.json  (a REVIEW file — NOT water_images.json).
Nothing here touches water_images.json. Every candidate must still be eyeballed
before it ships — the API can't see whether the photo actually shows the water.

Rate limit (anonymous): 20/min, 200/day. One pass over ~141 waters fits; the
script throttles to stay under the burst cap and is resumable.

Run:  python fetch_openverse_images.py
      python fetch_openverse_images.py --all   (re-check waters that already have an image)
Review: openverse_candidates.json
"""

import json
import re
import sys
import time
import requests

STOCKING_DATA   = "stocking_data.json"
EXISTING_IMAGES = "water_images.json"      # read-only: skip waters already covered
OUTPUT_FILE     = "openverse_candidates.json"

API_URL         = "https://api.openverse.org/v1/images/"
LICENSE_TYPE    = "commercial,modification"
PAGE_SIZE       = 20
THROTTLE_SEC    = 3.2                       # stay under 20/min anonymous burst
CANDIDATES_PER_WATER = 4

POSITIVE_KEYWORDS = ["lake", "reservoir", "river", "creek", "pond", "water",
                     "fishing", "shore", "shoreline", "boat", "scenic", "landscape",
                     "sunset", "sunrise", "state park", "dam"]
# Botanical / macro / wildlife survey shots score high on a name match but show
# no water (learned from the Wikimedia pass).
NEGATIVE_KEYWORDS = ["flower", "wildflower", "plant", "botany", "botanical", "macro",
                     "insect", "bug", "beetle", "spider", "moth", "butterfly",
                     "fungus", "mushroom", "lichen", "cactus", "closeup", "close-up",
                     "portrait", "map", "diagram", "logo", "aircraft", "airplane"]
# Other US states / regions — guard against same-named waters elsewhere
WRONG_PLACE = ["minnesota", " mn ", "wisconsin", "michigan", "colorado", " co ",
               "arizona", " az ", "texas", " tx ", "utah", "california", " ca ",
               "oregon", "washington", "montana", "idaho", "nevada", "wyoming",
               "oklahoma", "kansas", "pennsylvania", " pa ", "florida", "georgia",
               "canada", "scotland", "ireland", "australia"]
NEAR_PREFIXES = ["near ", "northeast of", "northwest of", "southeast of",
                 "southwest of", "north of", "south of", "east of", "west of"]

session = requests.Session()
session.headers.update({
    "User-Agent": "NMStockingReport/1.0 (https://stockingreport.com; stocking@kissmygrits.net)"
})


def query_for(name):
    """Build a search string: drop parentheticals, append the state."""
    base = re.sub(r"\([^)]*\)", "", name).strip()
    return f"{base} New Mexico"


def openverse_search(query):
    params = {
        "q":            query,
        "license_type": LICENSE_TYPE,
        "page_size":    PAGE_SIZE,
    }
    try:
        r = session.get(API_URL, params=params, timeout=20)
        if r.status_code == 429:
            print("  (rate limited — sleeping 60s)", end=" ", flush=True)
            time.sleep(60)
            r = session.get(API_URL, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        print(f"  search error: {e}", end=" ")
        return []


def score_candidate(item, water_name):
    w = item.get("width") or 0
    h = item.get("height") or 0
    if w < 800 or h < 400:
        return 0, None
    if h > w:                                   # want landscape for a wide banner
        return 0, None
    url = item.get("url") or ""
    if not url:
        return 0, None

    title = (item.get("title") or "").lower()
    tags  = " ".join(t.get("name", "") for t in (item.get("tags") or [])).lower()
    blob  = f"{title} {tags}"

    if any(neg in blob for neg in NEGATIVE_KEYWORDS):
        return 0, None
    if any(wp in f" {blob} " for wp in WRONG_PLACE):
        return 0, None
    if any(title.startswith(p) or p in title for p in NEAR_PREFIXES):
        return 0, None
    if not any(k in blob for k in POSITIVE_KEYWORDS):
        return 0, None

    score = 0
    name_words = re.sub(r"[()]", "", water_name.lower()).split()
    matched_name = False
    for word in name_words:
        if len(word) <= 3:
            continue
        if word in title:
            score += 10
            matched_name = True
        elif word in tags:
            score += 5
            matched_name = True
    if not matched_name:                        # nothing tying it to this water
        return 0, None

    if "new mexico" in blob or "nm" in tags.split():
        score += 6
    score += sum(2 for k in POSITIVE_KEYWORDS if k in blob)

    megapixels = (w * h) / 1_000_000
    score += min(int(megapixels), 5)
    ratio = w / h
    if ratio >= 1.5:
        score += 3
    if ratio >= 2.0:
        score += 2

    info = {
        "url":          url,
        "title":        item.get("title", ""),
        "source":       item.get("source", ""),
        "page_url":     item.get("foreign_landing_url", ""),   # link back to source
        "attribution":  item.get("attribution", "") or f"{item.get('creator','Unknown')} / {item.get('source','')}",
        "license":      f"{item.get('license','')} {item.get('license_version','')}".strip(),
        "width":        w,
        "height":       h,
        "score":        score,
    }
    return score, info


def find_candidates(name):
    scored = {}
    for item in openverse_search(query_for(name)):
        s, info = score_candidate(item, name)
        if info:
            key = info["url"]
            if key not in scored or s > scored[key][0]:
                scored[key] = (s, info)
    ranked = sorted(scored.values(), key=lambda t: t[0], reverse=True)
    return [info for _, info in ranked[:CANDIDATES_PER_WATER]]


def main():
    recheck_all = "--all" in sys.argv

    with open(STOCKING_DATA, encoding="utf-8") as f:
        stocking = json.load(f)
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
            print(f"[{i}/{total}] SKIP (searched): {name}")
            continue
        if not recheck_all and existing.get(name, {}).get("image"):
            print(f"[{i}/{total}] SKIP (has image): {name}")
            continue

        print(f"[{i}/{total}] Openverse: {name} ...", end=" ", flush=True)
        cands = find_candidates(name)
        results[name] = {"candidates": cands}
        print(f"{len(cands)} candidate(s)" + (f", top score {cands[0]['score']}" if cands else ""))

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        time.sleep(THROTTLE_SEC)

    with_cands = sum(1 for v in results.values() if v["candidates"])
    print("\n=== Done ===")
    print(f"  Waters searched:    {len(results)}")
    print(f"  With >=1 candidate: {with_cands}")
    print(f"Review {OUTPUT_FILE} — every image must be eyeballed before promoting.")


if __name__ == "__main__":
    main()
