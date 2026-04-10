"""
Build a review CSV for waters that currently have no map coordinates.
For each one, provides:
  - A Google Maps search link (best for finding the right spot manually)
  - The GNIS best-guess coords link (if GNIS found anything at all)
  - The GNIS match note (context on why it was excluded)

Output: geocoding/needs_review.csv
"""

import csv, json, urllib.parse

with open('gnis_results.csv', newline='', encoding='utf-8') as f:
    gnis = {r['water_name']: r for r in csv.DictReader(f)}

with open('../stocking_data.json', encoding='utf-8') as f:
    data = json.load(f)

# Waters that currently have no coords in stocking_data.json
no_coords = [name for name, info in sorted(data.items()) if not info.get('coords')]

rows = []
for name in no_coords:
    row = gnis.get(name, {})
    score = row.get('confidence', '')
    note = row.get('note', 'Not in GNIS results')
    gnis_lat = row.get('gnis_lat', '')
    gnis_lon = row.get('gnis_lon', '')

    # Google Maps search link — best starting point for manual review
    search_query = urllib.parse.quote(f"{name} New Mexico fishing")
    search_link = f"https://www.google.com/maps/search/{search_query}"

    # GNIS best-guess coord link (even if low confidence — just for reference)
    gnis_link = f"https://www.google.com/maps?q={gnis_lat},{gnis_lon}" if gnis_lat else ''

    rows.append({
        'water_name': name,
        'gnis_score': score,
        'gnis_note': note,
        'maps_search': search_link,
        'gnis_coords_link': gnis_link,
    })

with open('needs_review.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} waters to needs_review.csv")
for r in rows:
    print(f"  {r['water_name']:<55} score={r['gnis_score'] or '—'}")
