import requests
import json
import os
import shutil

# This script is for a one-time cleanup of the database to remove duplicates.

LIVE_DATA_URL = "https://stockingreport.com/stocking_data.json"
OUTPUT_FILE = "stocking_data.json"
BACKUP_FILE = "stocking_data.json.bak"

def clean_database():
    """
    Loads the live data, removes duplicates based on content (ignoring the URL),
    and saves a clean version.
    """
    print("--- Starting One-Time Database Cleanup ---")
    
    try:
        print(f"Loading existing data from {LIVE_DATA_URL}...")
        response = requests.get(LIVE_DATA_URL)
        response.raise_for_status()
        live_data = response.json()
        print("Successfully loaded live data.")
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"FATAL: Could not load or parse live data file. Error: {e}. Aborting cleanup.")
        return

    cleaned_data = {}
    total_removed = 0

    for water_body, data in live_data.items():
        unique_records = []
        seen_signatures = set()
        
        # Sort records by date to ensure the one we keep is the most recent version
        sorted_records = sorted(data.get("records", []), key=lambda r: r.get("date", ""), reverse=True)

        for record in sorted_records:
            # Create a "signature" of the record's content, ignoring the URL
            signature = (
                record.get("date"),
                record.get("species"),
                record.get("quantity"),
                record.get("length")
            )
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                unique_records.append(record)
            else:
                total_removed += 1
        
        # Add the cleaned list of records to our new data structure
        cleaned_data[water_body] = {"records": unique_records}

    print(f"\nCleanup complete. Removed {total_removed} duplicate records.")
    
    if cleaned_data:
        print("Saving the cleaned database...")
        try:
            # Create a backup of the final cleaned file before overwriting
            if os.path.exists(OUTPUT_FILE):
                shutil.copy(OUTPUT_FILE, BACKUP_FILE)
                print(f"Created backup of old file: {BACKUP_FILE}")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(cleaned_data, f, indent=4)
            print(f"Successfully saved cleaned data file: {OUTPUT_FILE}")
        except IOError as e:
            print(f"Error writing to file {OUTPUT_FILE}: {e}")
    else:
        print("No data was cleaned. The data file was not written.")

    print("--- Cleanup Finished ---")

if __name__ == "__main__":
    clean_database()
