# Deployment Guide

## Getting Your Changes Live

### Quick Overview
1. Commit your changes to Git
2. Push to GitHub
3. Render automatically rebuilds the site
4. Done! (Render takes ~2-3 minutes to deploy)

---

## Detailed Steps

### Step 1: Add Files to Git

```bash
# Add the important files
git add scraper.py
git add stocking_data_clean.json
git add stocking_data.json
git add public/
git add .github/
git add cleanup_data.py
git add weekly_update.py
git add rebuild_from_local.py
git add download_pdfs.py
git add DATA_MANAGEMENT.md
git add DEPLOYMENT.md
git add .gitignore
```

### Step 2: Commit Changes

```bash
git commit -m "Major update: Clean data + automated weekly updates

- Cleaned data: 201 → 179 water bodies
- Fixed parser for 2022+ PDF format
- Added weekly_update.py for incremental updates
- Added GitHub Actions workflow for automation
- Created cleanup_data.py to remove duplicates
- Total: 5,657 clean stocking records (2020-2025)"
```

### Step 3: Push to GitHub

```bash
git push origin main
```

### Step 4: Verify on GitHub

Go to: https://github.com/KylePetree68/StockingReport-website

You should see:
- Your commit message
- Green checkmark (files uploaded successfully)

### Step 5: Check Render Deployment

1. Go to your Render dashboard
2. Find your StockingReport-website project
3. You should see:
   - "Deploying..." (yellow)
   - Then "Live" with green checkmark (~2-3 minutes)

---

## What Happens Next?

### Automatic Weekly Updates

Every Monday at 6 AM UTC, GitHub Actions will:
1. Run `weekly_update.py`
2. Check for new stocking reports
3. Add any new data to `stocking_data_clean.json`
4. Regenerate static pages
5. Commit changes to GitHub
6. Trigger Render deployment automatically

### Manual Updates

If you want to update before Monday:

```bash
# Run the weekly update locally
python weekly_update.py

# Then commit and push
git add stocking_data_clean.json stocking_data.json public/
git commit -m "Manual data update"
git push origin main
```

---

## File Structure

### Files Committed to GitHub

```
StockingReport-website/
├── scraper.py                  # Core parser (improved)
├── stocking_data_clean.json    # Master clean data ⭐
├── stocking_data.json          # Compatibility copy
├── public/                     # Static HTML pages
│   ├── sitemap.xml
│   └── waters/*.html           # Individual water body pages
├── .github/workflows/          # GitHub Actions
│   └── weekly-update.yml       # Automated weekly updates
├── cleanup_data.py             # Data cleaning utility
├── weekly_update.py            # Incremental update script
├── rebuild_from_local.py       # Full rebuild script
├── download_pdfs.py            # PDF download utility
├── manual_coordinates.json     # GPS coordinate overrides
├── template.html               # Page template
└── build.sh                    # Render build script
```

### Files NOT Committed (in .gitignore)

```
downloaded_pdfs/                # Too large (~190 PDFs)
__pycache__/                    # Python cache
*.bak                           # Backups
test_*.py                       # Test scripts
.claude/                        # Claude Code config
```

---

## Troubleshooting

### Push is rejected

If you get "Updates were rejected":
```bash
git pull origin main --rebase
git push origin main
```

### Render doesn't rebuild

1. Check GitHub Actions tab - did the workflow run?
2. Check Render logs for errors
3. Manual trigger: Go to Render → "Manual Deploy"

### Data looks wrong on site

1. Check `stocking_data_clean.json` is correct
2. Run `python cleanup_data.py` to fix duplicates
3. Commit and push again

---

## Current Status

- **179** water bodies (cleaned)
- **5,657** stocking records
- **2020-2025** date range
- **Weekly automated updates** enabled
- **Render deployment** configured
