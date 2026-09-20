"""
Pull current park alerts (boat ramp closures, low-water notices, seasonal
closures) from the NM State Parks public alerts app and write
park_alerts.json keyed by park name.

Source: https://wwwapps.emnrd.nm.gov/SPD/ParksReportingPublicDisplay/Closure
(server-rendered HTML: one table per park with name, phone and a list of
alerts, each with posting dates).

Runs in the nightly GitHub Action. If the page cannot be fetched or parses
to nothing, the previous park_alerts.json is left untouched.

    python fetch_park_alerts.py
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

URL = "https://wwwapps.emnrd.nm.gov/SPD/ParksReportingPublicDisplay/Closure"
OUTPUT_FILE = "park_alerts.json"
HEADERS = {"User-Agent": "NMStockingReport/1.0 (stockingreport.com)"}

# Alerts that matter to an angler get flagged so the page can lead with them.
BOATING_RE = re.compile(r"\b(boat|ramp|launch|dock|water level|lake level|low water|no.wake|motor)", re.I)


def parse(html):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    parks = {}
    for table in main.find_all("table", recursive=False):
        name_cell = table.find("td", class_="bold")
        if not name_cell:
            continue
        park = " ".join(name_cell.get_text(" ", strip=True).split())
        phone_a = table.find("a", href=re.compile(r"^tel:"))
        phone = phone_a.get_text(strip=True) if phone_a else ""
        alerts = []
        inner = table.find("table")
        if inner:
            for cell in inner.find_all("td"):
                spans = cell.find_all("span")
                if not spans:
                    continue
                text = " ".join(spans[0].get_text(" ", strip=True).split())
                if not text:
                    continue
                posted = until = ""
                for sp in spans[1:]:
                    m = re.search(r"Posting Dates:\s*([\d/]+)\s*-\s*([\d/]+|ongoing)", sp.get_text(" ", strip=True))
                    if m:
                        posted, until = m.group(1), m.group(2)
                        break
                alerts.append({
                    "text": text,
                    "posted": posted,
                    "until": until,
                    "boating": bool(BOATING_RE.search(text)),
                })
        if alerts:
            parks[park] = {"phone": phone, "alerts": alerts}
    return parks


def main():
    try:
        r = requests.get(URL, headers=HEADERS, timeout=60)
        r.raise_for_status()
        parks = parse(r.text)
    except Exception as e:
        print(f"ERROR fetching park alerts: {type(e).__name__}: {e}")
        print("Leaving existing park_alerts.json untouched.")
        return 0 if os.path.exists(OUTPUT_FILE) else 1
    if not parks:
        print("Parsed zero parks - page layout may have changed. Leaving existing file untouched.")
        return 0 if os.path.exists(OUTPUT_FILE) else 1
    out = {
        "_fetched": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": URL,
    }
    out.update(dict(sorted(parks.items())))
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, ensure_ascii=False)
    n_alerts = sum(len(p["alerts"]) for p in parks.values())
    n_boat = sum(a["boating"] for p in parks.values() for a in p["alerts"])
    print(f"park_alerts.json: {len(parks)} parks, {n_alerts} alerts ({n_boat} boating-related).")
    for park, p in parks.items():
        for a in p["alerts"]:
            if a["boating"]:
                print(f"  BOAT {park}: {a['text'][:90]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
