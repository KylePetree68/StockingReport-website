#pip install -r requirements.txt
#python rebuild_database.py
#python scraper.py
#python clean_database.py
#
#!/bin/bash
# Render build script
# Note: Data updates are handled by GitHub Actions daily at 6:30 PM MT
# This script only needs to install dependencies since all data files
# (stocking_data_clean.json, public/waters/*.html) are already in the repo

mkdir -p public/waters
pip install -r requirements.txt

# Clean up build artifacts before Render uploads
rm -rf .venv
rm -rf __pycache__
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete
