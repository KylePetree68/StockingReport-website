import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pdfplumber
import io
import os
import shutil
import time

# This script is a targeted debugging tool to inspect a specific problematic PDF.

def extract_text_from_pdf(pdf_url):
    """
    Downloads a PDF from a URL and extracts all text from it.
    """
    print(f"  > Processing specific debug URL: {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=30)
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

def debug_specific_file():
    """
    DEBUGGING FUNCTION: Processes only the known problematic PDF from Oct 29, 2021,
    and prints its full text content to the log for analysis.
    """
    print("--- Starting SPECIFIC FILE DEBUG Job ---")
    
    # ** THE FIX IS HERE **
    # This URL has been corrected to the one you provided.
    target_pdf_url = "https://wildlife.dgf.nm.gov/download/stocking-report-10_29_21/?wpdmdl=44541"
    
    print(f"\nAttempting to process the report from October 29, 2021: {target_pdf_url}\n")
    
    raw_text = extract_text_from_pdf(target_pdf_url)

    if raw_text:
        print("-------------------- BEGIN PDF TEXT (Oct 29, 2021) --------------------")
        print(raw_text)
        print("--------------------  END PDF TEXT (Oct 29, 2021)  --------------------")
        print("\nDebug job finished. Please copy all the text between the BEGIN and END markers and paste it in your next reply.")
    else:
        print("\nFailed to extract any text from the target PDF. The file might be empty or unreadable.")


if __name__ == "__main__":
    # We are calling the special debugging function.
    debug_specific_file()
