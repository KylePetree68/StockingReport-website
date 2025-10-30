#pip install -r requirements.txt
#python rebuild_database.py
#python scraper.py
#python clean_database.py
#
#!/bin/bash
mkdir -p public/waters
pip install -r requirements.txt
python scraper.py

# Clean up build artifacts before Render uploads
rm -rf .venv
rm -rf __pycache__
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete
