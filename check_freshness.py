"""
check_freshness.py

Early-warning canary for the stocking pipeline. NMDGF publishes a new stocking
report almost exactly weekly (occasionally an 8-day gap). If the newest report
we have is older than STALE_DAYS, something is probably wrong -- the scraper
broke, NMDGF changed their site/URL scheme, or publishing stopped -- and we want
to be told instead of silently going stale.

Exit code:
    0 = fresh (newest report within STALE_DAYS)
    1 = stale  (the freshness-check workflow turns this into a GitHub issue)

STALE_DAYS: default 10. Normal cadence is 7 days with an occasional 8, so 10
means "clearly abnormal" without false alarms. Set it to 8 for a tighter alarm
(expect an occasional false positive on normal 8-day gaps), or via the
STALE_DAYS environment variable.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

STALE_DAYS = int(os.environ.get("STALE_DAYS", "10"))
DATA_FILE = "stocking_data.json"

REPORT_DATE_RE = re.compile(r"stocking-report-(\d{1,2})[_-](\d{1,2})[_-](\d{2,4})")


def newest_report_date(data):
    """Newest report date parsed from the reportUrl slugs (stocking-report-MM_DD_YY)."""
    newest = None
    for water in data.values():
        for record in water.get("records", []):
            m = REPORT_DATE_RE.search(record.get("reportUrl", ""))
            if not m:
                continue
            mm, dd, yy = m.groups()
            yy = int(yy)
            yy = 2000 + yy if yy < 100 else yy
            try:
                d = datetime(yy, int(mm), int(dd)).date()
            except ValueError:
                continue
            if newest is None or d > newest:
                newest = d
    return newest


def main():
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    newest = newest_report_date(data)
    today = datetime.now(timezone.utc).date()

    if newest is None:
        print("ALERT: no parseable report date found in stocking_data.json.")
        # The '::stale::' marker line is what the workflow puts into the issue body.
        print("::stale::No parseable NMDGF report dates were found in the data at all "
              "-- the scraper or NMDGF's URL scheme may have changed.")
        sys.exit(1)

    age = (today - newest).days
    print(f"Newest report: {newest} | age: {age} day(s) | threshold: {STALE_DAYS} days")

    if age > STALE_DAYS:
        print(f"ALERT: no new stocking report in {age} days.")
        print(f"::stale::No new NMDGF stocking report in {age} days "
              f"(newest is {newest}; threshold is {STALE_DAYS}). "
              f"Reports normally arrive weekly, so this likely means the scraper "
              f"(weekly_update.py) broke or NMDGF changed their site.")
        sys.exit(1)

    print("OK: data is fresh.")
    sys.exit(0)


if __name__ == "__main__":
    main()
