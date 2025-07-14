import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os
import shutil

# This script is for a one-time debug to inspect an old PDF file.

BASE_URL = "https://wildlife.dgf.nm.gov"
ARCHIVE_PAGE_URL = f"{BASE_URL}/fishing/weekly-report/fish-stocking-archive/"

def get_pdf_links(page_url):
    """
    Scrapes the archive page to find links to all available PDF reports.
    """
    print(f"Finding all PDF links on the archive page: {page_url}...")
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
        print(f"Found {len(pdf_links)} total PDF links in the archive.")
        return pdf_links
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_url}: {e}")
        return []

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    """
    print(f"  > Processing {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=20) # Increased timeout
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)
        full_text = ""
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=False)
                if page_text:
                    full_text += page_text + "\n"
        return full_text
    except Exception as e:
        print(f"    [!] Failed to extract text from {pdf_url}: {e}")
        return ""

def debug_old_file():
    """
    DEBUGGING FUNCTION: Finds an old PDF (from 2024), extracts its text,
    and prints it to the log for analysis.
    """
    print("--- Starting OLD FILE DEBUG Job ---")
    print("This script will find a 2024 report and print its content.")

    all_pdf_links = get_pdf_links(ARCHIVE_PAGE_URL)
    if not all_pdf_links:
        print("\nCould not find any PDF links. Aborting.")
        return

    # **IMPROVED LOGIC TO FIND AN OLDER FILE**
    target_pdf_url = None
    # A more robust way to find a report from a previous year.
    # Look for "-24" which is common in date formats like "12-25-24" in the URL.
    for link in all_pdf_links:
        if "-24" in link:
            target_pdf_url = link
            break
    
    if not target_pdf_url:
         # Fallback if the first check fails, look for the full year
        for link in all_pdf_links:
            if "2024" in link:
                target_pdf_url = link
                break

    # If we still can't find a 2024 file, grab an older one from the list to debug.
    if not target_pdf_url and len(all_pdf_links) > 10:
        print("\nCould not find a 2024 report specifically. Grabbing an older report from the archive to debug...")
        # Grab the 10th from the end, which is likely from a previous year.
        target_pdf_url = all_pdf_links[-10] 

    if not target_pdf_url:
        print("\nCould not find a suitable old report to debug. Please check the archive page.")
        return

    print(f"\nFound a target report to debug: {target_pdf_url}\n")
    
    raw_text = extract_text_from_pdf(target_pdf_url)

    if raw_text:
        print("-------------------- BEGIN OLD PDF TEXT --------------------")
        print(raw_text)
        print("--------------------  END OLD PDF TEXT  --------------------")
        print("\nDebug job finished. Please copy all the text between the BEGIN and END markers and paste it in your next reply.")
    else:
        print("\nFailed to extract any text from the target PDF. The file might be empty or unreadable.")


if __name__ == "__main__":
    # We are calling the special debugging function instead of the main one.
    debug_old_file()
