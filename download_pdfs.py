import os
import requests
from bs4 import BeautifulSoup
import time

os.makedirs('downloaded_pdfs', exist_ok=True)

def download_pdf(url, filename):
    """Download a PDF"""
    filepath = os.path.join('downloaded_pdfs', filename)
    
    if os.path.exists(filepath):
        print(f"  ⊘ Skip: {filename}")
        return True
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"  ✓ Downloaded: {filename}")
            return True
        else:
            print(f"  ✗ Failed: {filename}")
            return False
    except Exception as e:
        print(f"  ✗ Error: {filename} - {e}")
        return False

# Collect all unique PDF URLs
all_pdf_urls = {}  # Use dict to auto-dedupe by filename

print("=== Scraping archive pages ===\n")

for page_num in range(1, 20):
    if page_num == 1:
        url = "https://wildlife.dgf.nm.gov/fishing/weekly-report/fish-stocking-archive/"
    else:
        url = f"https://wildlife.dgf.nm.gov/fishing/weekly-report/fish-stocking-archive/?cp={page_num}"
    
    print(f"Page {page_num}: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all download links
        found = 0
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/download/stocking-report-' in href and '?wpdmdl=' in href:
                # Extract clean filename from URL
                # Example: /download/stocking-report-8-29-25/?wpdmdl=...
                parts = href.split('/download/')[1].split('?')[0].strip('/')
                filename = parts + '.pdf'
                
                # Store with filename as key (auto-dedupes)
                if filename not in all_pdf_urls:
                    all_pdf_urls[filename] = href
                    found += 1
        
        print(f"  Found {found} new PDFs")
        
        if found == 0:
            print("  No new PDFs, stopping.")
            break
            
    except Exception as e:
        print(f"  Error: {e}")
        break
    
    time.sleep(1)

print(f"\n=== Total unique PDFs: {len(all_pdf_urls)} ===\n")

# Download all
print("=== Downloading ===\n")
success = 0
for i, (filename, url) in enumerate(all_pdf_urls.items(), 1):
    print(f"[{i}/{len(all_pdf_urls)}] {filename}")
    if download_pdf(url, filename):
        success += 1
    time.sleep(0.5)

print(f"\n=== Done: {success}/{len(all_pdf_urls)} downloaded ===")