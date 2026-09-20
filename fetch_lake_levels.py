"""
Fetch daily reservoir water-surface elevation for the waters listed in
lake_level_sources.json and write lake_levels.json.

Sources: USGS NWIS daily values, Bureau of Reclamation UC hydrodata daily
CSV, and the Army Corps CDA API (15-minute data aggregated to one value per
day). Keeps the last HISTORY_DAYS days so the water page can chart a full
year with some margin.

Runs in the nightly GitHub Action. A source that fails keeps whatever was
in lake_levels.json from the previous run, so one bad night never blanks a
chart; the page shows the data's own date.

    python fetch_lake_levels.py
"""
import json
import os
import sys
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone

import requests

SOURCES_FILE = "lake_level_sources.json"
OUTPUT_FILE = "lake_levels.json"
HISTORY_DAYS = 400
HEADERS = {"User-Agent": "NMStockingReport/1.0 (stockingreport.com)"}
TIMEOUT = 60

USGS_DV = "https://waterservices.usgs.gov/nwis/dv/"
USBR_CSV = "https://www.usbr.gov/uc/water/hydrodata/reservoir_data/{id}/csv/49.csv"
USACE_TS = "https://water.usace.army.mil/cda/reporting/providers/spa/timeseries"


def _cutoff():
    return (date.today() - timedelta(days=HISTORY_DAYS)).isoformat()


def fetch_usgs(site, param):
    r = requests.get(USGS_DV, params={
        "format": "json", "sites": site, "parameterCd": param,
        "startDT": _cutoff(), "endDT": date.today().isoformat(),
    }, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    series = r.json()["value"]["timeSeries"]
    best = []
    for ts in series:
        vals = [(v["dateTime"][:10], float(v["value"])) for v in ts["values"][0]["value"]
                if v.get("value") not in (None, "", "-999999")]
        if len(vals) > len(best):
            best = vals
    return best


def parse_usbr_csv(text):
    """'datetime,pool elevation' header then 'YYYY-MM-DD,value' rows."""
    out = []
    cutoff = _cutoff()
    for line in text.splitlines()[1:]:
        parts = line.strip().split(",")
        if len(parts) < 2 or parts[0] < cutoff:
            continue
        try:
            out.append((parts[0], float(parts[1])))
        except ValueError:
            continue
    return out


def fetch_usbr(site_id):
    r = requests.get(USBR_CSV.format(id=site_id), headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return parse_usbr_csv(r.text)


def fetch_usace(tsid):
    begin = datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)
    r = requests.get(USACE_TS, params={
        "name": tsid,
        "begin": begin.strftime("%Y-%m-%dT00:00:00Z"),
        "end": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    # 15-minute readings -> last reading of each UTC day
    daily = OrderedDict()
    for t, v in r.json().get("values", []):
        if v is None:
            continue
        daily[t[:10]] = float(v)
    return list(daily.items())


def load_previous():
    if not os.path.exists(OUTPUT_FILE):
        return {}
    try:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            return {k: v for k, v in json.load(f).items() if not k.startswith("_")}
    except Exception:
        return {}


def main():
    with open(SOURCES_FILE, encoding="utf-8") as f:
        sources = {k: v for k, v in json.load(f).items() if not k.startswith("_")}
    previous = load_previous()
    out = {"_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "_readme": "Daily water-surface elevation (ft) or storage (ac-ft) per reservoir; see lake_level_sources.json. Written by fetch_lake_levels.py."}
    ok = failed = 0
    for name, src in sources.items():
        try:
            if src["source"] == "usgs":
                series = fetch_usgs(src["id"], src.get("param", "62614"))
            elif src["source"] == "usbr":
                series = fetch_usbr(src["id"])
            elif src["source"] == "usace":
                series = fetch_usace(src["id"])
            else:
                raise ValueError(f"unknown source {src['source']}")
            if not series:
                raise ValueError("empty series")
            series = sorted(series)[-HISTORY_DAYS:]
            out[name] = {
                "source": src["source"], "label": src.get("label", ""), "unit": src.get("unit", "ft"),
                "full_pool_ft": src.get("full_pool_ft"),
                "fetched": date.today().isoformat(),
                "series": [[d, round(v, 2)] for d, v in series],
            }
            ok += 1
            print(f"  OK   {name:24} {len(series):3} days, latest {series[-1][0]} = {series[-1][1]} {src.get('unit','ft')}")
        except Exception as e:
            failed += 1
            if name in previous:
                out[name] = previous[name]
                print(f"  KEEP {name:24} fetch failed ({type(e).__name__}: {str(e)[:60]}); kept data from {previous[name].get('fetched')}")
            else:
                print(f"  FAIL {name:24} {type(e).__name__}: {str(e)[:80]}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    print(f"\nlake_levels.json: {ok} fetched, {failed} failed/kept.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
