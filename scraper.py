import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

# The URL of the main stocking report page
URL = "https://wildlife.dgf.nm.gov/fishing/weekly-report/"
# The file where the final JSON data will be saved
OUTPUT_FILE = "stocking_data.json"

def parse_stocking_info(text_blob):
    """
    Parses a block of text to extract individual stocking records.
    This is the core logic for interpreting the report text.
    """
    records = []
    # Regex to find dates in "Month Day" format, followed by stocking details.
    # It looks for a date, then captures everything until the next date or the end of the string.
    date_pattern = re.compile(r"(\w+\s\d+):\s(.*?)(?=\w+\s\d+:|\Z)", re.DOTALL)
    
    # Regex to extract details from each stocking entry
    # Example: "Stocked 1,250 channel catfish (18-inch)."
    stock_pattern = re.compile(r"(\d{1,3}(?:,\d{3})*)\s([\w\s-]+?)\s\((\d{1,2}\.?\d*)-inch\)")

    for match in date_pattern.finditer(text_blob):
        date_str, entry_text = match.groups()
        
        # Assume the current year for the date
        current_year = datetime.now().year
        try:
            # Combine month, day, and year, then format to YYYY-MM-DD
            full_date = datetime.strptime(f"{date_str} {current_year}", "%B %d %Y")
            formatted_date = full_date.strftime("%Y-%m-%d")
        except ValueError:
            # If date parsing fails, skip this record
            print(f"Warning: Could not parse date '{date_str}'. Skipping.")
            continue

        # Find all individual stocking events within this date's entry
        for stock_match in stock_pattern.finditer(entry_text):
            quantity, species, length = stock_match.groups()
            
            # Clean up the data
            quantity = quantity.replace(',', '')
            species = species.strip().title() # Capitalize species name
            
            # For now, hatchery is not consistently available in the text, so we'll mark it N/A
            record = {
                "date": formatted_date,
                "species": species,
                "quantity": quantity,
                "length": length,
                "hatchery": "N/A" 
            }
            records.append(record)
            
    return records

def scrape_reports():
    """
    Main function to scrape the NMDGF website and generate the JSON file.
    """
    print("Starting scrape of NMDGF website...")
    final_data = {}

    try:
        response = requests.get(URL)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {URL}: {e}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find the main content area of the page
    content_div = soup.find("div", class_="entry-content")
    if not content_div:
        print("Error: Could not find the main content div.")
        return

    # The reports are structured with <h3> tags for region names
    # and <p> tags for the content, including water body names in <strong> tags.
    for region_header in content_div.find_all("h3"):
        # The content for a region is in the following <p> tags until the next <h3>
        current_element = region_header.find_next_sibling()
        while current_element and current_element.name != 'h3':
            if current_element.name == 'p':
                # Find the water body name, which is usually in a <strong> tag
                water_body_tag = current_element.find("strong")
                if water_body_tag:
                    water_body_name = water_body_tag.get_text(strip=True).replace(":", "")
                    
                    # The rest of the <p> tag contains the stocking details
                    stocking_text = current_element.get_text(strip=True)
                    
                    # Remove the water body name from the text to isolate stocking info
                    stocking_text = stocking_text.replace(water_body_tag.get_text(strip=True), "").strip()

                    records = parse_stocking_info(stocking_text)
                    
                    if records:
                        print(f"Found {len(records)} records for {water_body_name}")
                        if water_body_name not in final_data:
                            final_data[water_body_name] = {
                                "reportUrl": URL,
                                "records": []
                            }
                        final_data[water_body_name]["records"].extend(records)

            current_element = current_element.find_next_sibling()
            
    # Save the scraped data to the output file
    if final_data:
        try:
            with open(OUTPUT_FILE, "w") as f:
                json.dump(final_data, f, indent=4)
            print(f"\nSuccessfully scraped {len(final_data)} water bodies.")
            print(f"Data saved to {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("No stocking data found. The output file was not updated.")

if __name__ == "__main__":
    scrape_reports()
