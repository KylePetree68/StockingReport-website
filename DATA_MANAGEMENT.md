# Stocking Data Management

## Overview

This project uses a **master JSON + incremental updates** approach:

1. **Master Data**: `stocking_data_clean.json` - Clean, deduplicated historical data (2020-present)
2. **Weekly Updates**: Automated script fetches only new reports
3. **GitHub Actions**: Auto-runs weekly and commits changes

## Files

- `stocking_data_clean.json` - **Master data file** (use this!)
- `stocking_data.json` - Legacy compatibility file
- `weekly_update.py` - Incremental update script
- `cleanup_data.py` - Data cleaning/deduplication utility
- `rebuild_from_local.py` - Full rebuild from downloaded PDFs
- `scraper.py` - Core parsing logic

## Workflow

### Initial Setup (One-time)

The master data has already been created with historical data from 2020-2025:
- 179 water bodies
- 5,657 stocking records
- All duplicates removed
- Malformed entries cleaned

### Weekly Updates (Automated)

GitHub Actions runs `weekly_update.py` every Monday at 6 AM UTC:

1. Loads existing `stocking_data_clean.json`
2. Fetches latest PDFs from first archive page
3. Identifies NEW reports not yet processed
4. Parses and adds new records
5. Regenerates static pages and sitemap
6. Commits changes to GitHub

### Manual Update

To run an update manually:

```bash
python weekly_update.py
```

### Full Rebuild

If you need to rebuild everything from scratch:

```bash
# 1. Download all PDFs (takes ~10 minutes)
python download_pdfs.py

# 2. Rebuild from local PDFs (takes ~15 minutes)
python rebuild_from_local.py

# 3. Clean up duplicates
python cleanup_data.py
```

## Data Quality

### Automated Cleanup

The `cleanup_data.py` script removes:
- Malformed water names (with hatchery fragments)
- Incomplete names (missing closing parentheses)
- Duplicate entries (same water body, different spelling)

### Known Issues

Some water names may still need manual correction in `manual_coordinates.json`:
- GPS coordinates for unmapped locations
- Name standardization (e.g., "Lake" vs "Reservoir")

## Deployment

### Render.com

The site is deployed as a static site on Render:
- Automatically rebuilds when GitHub main branch updates
- GitHub Actions pushes weekly data updates
- Render deploys the updated static files

### GitHub Repository

All files sync to GitHub:
- Data files: `stocking_data_clean.json`, `stocking_data.json`
- Static pages: `public/waters/*.html`
- Sitemap: `public/sitemap.xml`

## Monitoring

Check GitHub Actions tab to monitor weekly updates:
- Green checkmark = successful update
- Red X = failed (check logs)
- Yellow dot = running

## Troubleshooting

### No new data after update

This is normal if no new reports were published. The script will show:
```
✓ No new reports to process. Data is already up-to-date!
```

### Duplicate water bodies appearing

Run the cleanup script:
```bash
python cleanup_data.py
```

### Missing coordinates

Add to `manual_coordinates.json`:
```json
{
  "Water Body Name": {
    "lat": 35.1234,
    "lon": -106.5678
  }
}
```

## Statistics

Current as of last rebuild:
- **179** unique water bodies
- **5,657** stocking records
- **293** static HTML pages
- Date range: 2020-01-03 to 2025-10-31
