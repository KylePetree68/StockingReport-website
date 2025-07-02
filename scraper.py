import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os

# The URL of the main stocking report page
BASE_URL = "https://wildlife.dgf.nm.gov"
REPORTS_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/"
# The file where the final JSON data will be saved and read from
OUTPUT_FILE = "stocking_data.json"

def get_pdf_links(page_url):
    """
    Scrapes the main reports page to find links to individual PDF reports
    that explicitly contain "Stocking Report" in the link text.
    """
    print(f"Finding 'Stocking Report' PDF links on {page_url}...")
    pdf_links = []
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        for a_tag in soup.find_all("a", href=True, string=re.compile("Stocking Report", re.IGNORECASE)):
            if "?wpdmdl=" in a_tag['href']:
                full_url = a_tag['href']
                if not full_url.startswith('http'):
                    full_url = f"{BASE_URL}{full_url}"
                pdf_links.append(full_url)

        print(f"Found {len(pdf_links)} relevant PDF links.")
        return pdf_links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_url}: {e}")
        return []

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    """
    print(f"  > Extracting text from {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=15)
        response.raise_for_status()
        
        pdf_file = io.BytesIO(response.content)
        
        full_text = ""
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def scrape_reports_debug():
    """
    DEBUGGING FUNCTION: This function will only process the first PDF it finds,
    print all of its extracted text to the log, and then stop.
    This is to help us see the exact text structure the script is working with.
    """
    print("--- Starting DEBUG Scrape Job ---")
    print("This script will now attempt to download the first PDF and print its content.")

    pdf_links = get_pdf_links(REPORTS_PAGE_URL)
    if not pdf_links:
        print("\nCould not find any PDF links. Aborting.")
        return

    # Process only the most recent report for debugging
    first_pdf_url = pdf_links[0]
    print(f"\nProcessing first PDF found: {first_pdf_url}\n")
    
    raw_text = extract_text_from_pdf(first_pdf_url)

    if raw_text:
        print("-------------------- BEGIN PDF TEXT --------------------")
        print(raw_text)
        print("--------------------  END PDF TEXT  --------------------")
        print("\nDebug job finished. Please copy all the text between the BEGIN and END markers and paste it in your next reply.")
    else:
        print("\nFailed to extract any text from the PDF. The file might be empty or unreadable.")

if __name__ == "__main__":
    # We are calling the special debugging function instead of the main one.
    scrape_reports_debug()
