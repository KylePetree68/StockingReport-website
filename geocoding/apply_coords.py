"""
Apply verified GNIS coordinates to stocking_data.json.

Conservative policy: only apply coords we're highly confident in.
  - Score 10: GNIS exact match, minus known-bad matches (wrong county / name collision)
  - Score 9:  selected near-exact matches (verified county)
  - Score 7:  manually computed river-segment midpoints
  - Score <=8 (except above): no pin — better to omit than misdirect

Run from the geocoding/ directory:
    python apply_coords.py
"""

import csv, json

# ── Waters where GNIS matched the WRONG feature ─────────────────────────────
# Same name exists in multiple NM counties, or matched a completely different body of water.
# Reason is documented inline for future reference.
GNIS_EXCLUSIONS = {
    "Animas River",
        # GNIS → Hidalgo Co. (southern tip of NM); stocking is near Aztec/Farmington (San Juan Co.)
    "Bataan Lake",
        # GNIS → Tank Lake, Quay Co.; should be Las Cruces area (Doña Ana Co.)
    "Bottomless Lakes",
        # GNIS → 32.06°N; Bottomless Lakes State Park is at 33.34°N near Roswell
    "Bosque Redondo",
        # GNIS → Spring, McKinley Co.; should be Fort Sumner (De Baca Co.)
    "Brantley Dam",
        # GNIS → Brantley Tank, Otero Co.; reservoir is in Eddy Co. near Carlsbad
    "Brazos River",
        # GNIS → Mancos River, San Juan Co.; Brazos River is near Chama (Rio Arriba Co.)
    "Caballo Lake",
        # GNIS → Caballo Lake, Quay Co. (name collision); stocking is Sierra Co. near Truth or Consequences
    "Chaparral Park Lake",
        # GNIS → Chaparral Lake, Cibola Co.; should be Doña Ana Co. (Las Cruces city park)
    "Clear Creek (Jemez Mts)",
        # GNIS → Clear Creek, Colfax Co.; Jemez Mountains are in Sandoval/Rio Arriba
    "Conservancy Park Lake (Aka Tingley Beach)",
        # GNIS → Park Lake, Guadalupe Co.; should be Albuquerque (Bernalillo Co.)
    "Cow Creek",
        # GNIS 33.04°N vs existing 36.64°N — 3.6° latitude difference; uncertain which is right
    "Coyote Creek (Nr Guadalupita)",
        # GNIS → Coyote Creek, Rio Arriba Co.; Guadalupita is in Mora Co.
    "El Rito Creek (Trib Of Chama)",
        # GNIS → El Rito Creek, Guadalupe Co.; the Chama tributary is in Rio Arriba Co.
    "Eunice Lake",
        # GNIS → Fence Lake, Cibola Co.; Eunice Lake is in Lea Co. near the TX border
    "Goose Lake",
        # GNIS → Grant Co. (33.02°N); existing coords show Taos/Colfax area (36.64°N)
    "Grindstone Reservoir",
        # GNIS → Stone Lake, Union Co.; should be Lincoln Co. near Ruidoso
    "Jal Lake",
        # GNIS → Juan Lake, Chaves Co.; Jal Lake is in Lea Co. in the town of Jal
    "Jemez River",
        # GNIS → river mouth near Bernalillo (35.37°N); stocking is upstream near Jemez Pueblo
    "Los Pinos River",
        # GNIS → San Juan Co. (107.60°W); stocking is likely near Chama (Rio Arriba, ~106.2°W)
    "Lost Lake",
        # GNIS 33.27°N vs existing 35.85°N — 2.6° latitude difference; uncertain
    "Manzano Lake",
        # GNIS → Manzanita Lake, Lincoln Co.; Manzano Lake is near the Manzano Mountains (Torrance Co.)
    "Navajo Reservoir",
        # GNIS → 107.05°W (Rio Arriba Co.); dam/main body is at 107.61°W (San Juan Co.)
    "Oasis Park Lake",
        # GNIS → Park Lake, Guadalupe Co.; Oasis Park Lake is in Clovis (Curry Co.)
    "Pecos River (Pecos Canyon)",
        # GNIS → Pecos River Canal, Eddy Co. at 32.69°N; Pecos Canyon is at ~35.7°N
    "Pecos River (Vill Of Pecos - Villanueva)",
        # GNIS → Pecos River Canal, Eddy Co. at 32.69°N; Villanueva is at ~35.5°N
    "Peralta Drain",
        # GNIS → Fera Drain, Sierra Co.; Peralta Drain is in the Albuquerque metro area
    "Ruidoso River",
        # GNIS → Red River, Taos Co.; Ruidoso River is in Lincoln Co. near Ruidoso
    "San Juan River (Quality)",
        # GNIS → drain in Socorro Co.; Quality Waters are in San Juan Co. near Aztec/Farmington
    "Taos Creek (Aka Rio Fernando De Taos)",
        # GNIS → Patos Creek, Lincoln Co.; Taos Creek is in Taos County
    "Willow Creek (Gila Drainage)",
        # GNIS → Willow Creek, Rio Arriba Co.; Gila Drainage is in Grant/Catron Co.
    "Willow Creek (Pecos Drainage)",
        # GNIS → Willow Creek, Rio Arriba Co.; Pecos Drainage is in San Miguel/Santa Fe Co.
}

# Score-9 near-exact matches we've verified are correct
APPROVED_SCORE_9 = {
    "Joe Vigil Lake",    # "Jose Vigil Lake" in Mora Co. — plausible name variant
    "Trees Lake",        # "Tree Lake" in Mora Co. — plausible name variant
    "White Water Creek", # "Whitewater Creek" in Hidalgo Co. — Glenwood area, correct
}

# ── Main ─────────────────────────────────────────────────────────────────────

with open('gnis_results.csv', newline='', encoding='utf-8') as f:
    rows = {r['water_name']: r for r in csv.DictReader(f)}

with open('../stocking_data.json', encoding='utf-8') as f:
    data = json.load(f)

# Load human-verified coords — these are never overwritten or cleared.
# Same file the scraper uses: manual_coordinates.json in the project root.
with open('../manual_coordinates.json', encoding='utf-8') as f:
    manual = json.load(f)

applied = []
manual_applied = []
cleared = []
skipped = []

for water_name, info in data.items():
    had_coords = bool(info.get('coords'))

    # ── Human-verified coords take absolute priority ──────────────────────────
    if water_name in manual:
        data[water_name]['coords'] = manual[water_name]
        manual_applied.append(water_name)
        continue

    if water_name not in rows:
        skipped.append((water_name, 'Not in GNIS results'))
        continue

    row = rows[water_name]
    score = float(row['confidence']) if row['confidence'] else 0
    accept = False

    if water_name in GNIS_EXCLUSIONS:
        reason = 'Known bad GNIS match (wrong county or wrong feature)'
    elif not row['gnis_lat']:
        reason = 'No GNIS match found'
    elif score == 10:
        accept = True
        reason = 'Score 10 — exact GNIS match'
    elif score == 9 and water_name in APPROVED_SCORE_9:
        accept = True
        reason = 'Score 9 — near-exact, verified correct'
    elif score == 7:
        accept = True
        reason = 'Score 7 — manually computed river-segment midpoint'
    else:
        reason = f'Score {score:.0f} — below confidence threshold (not verified)'

    if accept:
        data[water_name]['coords'] = {
            'lat': float(row['gnis_lat']),
            'lon': float(row['gnis_lon']),
        }
        applied.append((water_name, reason))
    else:
        if had_coords:
            del data[water_name]['coords']
            cleared.append((water_name, reason))
        else:
            skipped.append((water_name, reason))

with open('../stocking_data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

# ── Report ────────────────────────────────────────────────────────────────────

if manual_applied:
    print(f"\n{'MANUAL (human-verified) — highest priority':=<70}")
    for name in sorted(manual_applied):
        print(f"  {name}")

print(f"\n{'APPLIED (GNIS) — on map':=<70}")
for name, reason in sorted(applied):
    print(f"  {name:<55} {reason}")

print(f"\n{'CLEARED — had coords, now removed':=<70}")
for name, reason in sorted(cleared):
    print(f"  {name:<55} {reason}")

print(f"\n{'SKIPPED — no coords assigned':=<70}")
for name, reason in sorted(skipped):
    print(f"  {name:<55} {reason}")

print(f"""
{'SUMMARY':=<70}
  Manual (human):     {len(manual_applied):>3} waters  (never overwritten)
  GNIS (auto):        {len(applied):>3} waters
  Total on map:       {len(manual_applied) + len(applied):>3} waters
  No pin:             {len(cleared) + len(skipped):>3} waters

Add confirmed locations to manual_coordinates.json to protect them.
""")
