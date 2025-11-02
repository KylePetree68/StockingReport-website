#!/usr/bin/env python3
"""
Fetch fishing regulations from NM Game & Fish ArcGIS Feature Service.

This script queries the ArcGIS REST API to get regulation data for:
- Special Trout Waters (Lakes and Streams)
- Trophy Bass Waters
- Summer Catfish Waters
- Boat Ramps

Data is saved to regulations_data.json for use by the website.
"""

import requests
import json
from typing import Dict, List, Any
import time


# ArcGIS Feature Service base URL
BASE_URL = "https://services2.arcgis.com/CjbW1bVhK4dB3WOa/arcgis/rest/services/Fishing_Waters_Map_WFL1/FeatureServer"

# Layer IDs from the feature service
LAYERS = {
    "Trophy_Bass_Waters": 0,
    "Summer_Catfish_Waters": 1,
    "Boat_Ramps": 2,
    "Special_Trout_Waters_Streams": 3,
    "Special_Trout_Waters_Lakes": 4,
    "Habitat_Improvements_Streams": 5,
    "Habitat_Improvements_Lakes": 6
}


def query_layer(layer_id: int, layer_name: str) -> List[Dict[str, Any]]:
    """
    Query a single ArcGIS layer and return all features.

    Args:
        layer_id: The numeric ID of the layer
        layer_name: Human-readable name for logging

    Returns:
        List of feature attributes (dicts)
    """
    url = f"{BASE_URL}/{layer_id}/query"

    params = {
        "where": "1=1",  # Get all records
        "outFields": "*",  # All fields
        "returnGeometry": "false",  # We don't need geometry, just attributes
        "f": "json",
        "resultRecordCount": 2000  # Max records per request
    }

    try:
        print(f"Querying {layer_name} (layer {layer_id})...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "features" not in data:
            print(f"  No features found in {layer_name}")
            return []

        features = data["features"]
        records = [feature["attributes"] for feature in features]
        print(f"  Found {len(records)} records")

        return records

    except requests.exceptions.RequestException as e:
        print(f"  Error querying {layer_name}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"  Error parsing JSON for {layer_name}: {e}")
        return []


def normalize_water_name(name: str) -> str:
    """
    Normalize water body name for matching.

    - Convert to lowercase
    - Strip whitespace
    - Remove common suffixes/prefixes that might differ

    Args:
        name: Water body name

    Returns:
        Normalized name
    """
    if not name:
        return ""

    name = name.lower().strip()

    # Remove common variations
    replacements = [
        (" lake", ""),
        (" reservoir", ""),
        (" pond", ""),
        (" river", ""),
        (" creek", ""),
        (" stream", "")
    ]

    # Try both with and without suffix for matching
    return name


def fetch_all_regulations() -> Dict[str, Any]:
    """
    Fetch all regulation data from ArcGIS Feature Service.

    Returns:
        Dict with structure:
        {
            "special_trout_waters_lakes": [...],
            "special_trout_waters_streams": [...],
            "trophy_bass_waters": [...],
            "summer_catfish_waters": [...],
            "boat_ramps": [...],
            "metadata": {
                "last_updated": "2024-11-01T12:00:00",
                "source": "NM Game & Fish ArcGIS Feature Service",
                "total_records": 123
            }
        }
    """
    all_data = {}
    total_records = 0

    print("\n" + "="*60)
    print("Fetching Fishing Regulations from ArcGIS")
    print("="*60 + "\n")

    # Query each layer
    for layer_name, layer_id in LAYERS.items():
        records = query_layer(layer_id, layer_name)
        all_data[layer_name.lower()] = records
        total_records += len(records)
        time.sleep(0.5)  # Be nice to the API

    # Add metadata
    all_data["metadata"] = {
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "NM Game & Fish ArcGIS Feature Service",
        "source_url": BASE_URL,
        "total_records": total_records,
        "layers_queried": list(LAYERS.keys())
    }

    print(f"\n{'='*60}")
    print(f"Total records fetched: {total_records}")
    print(f"{'='*60}\n")

    return all_data


def build_water_lookup(regulations_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a lookup table by water body name for easy matching.

    Args:
        regulations_data: Raw data from fetch_all_regulations()

    Returns:
        Dict mapping water body names to their regulations:
        {
            "Alto Lake": {
                "special_trout_water": {...},
                "trophy_bass": {...},
                "boat_ramp": {...}
            }
        }
    """
    lookup = {}

    # Process Special Trout Waters - Lakes
    for record in regulations_data.get("special_trout_waters_lakes", []):
        water_name = record.get("Water_Name", "").strip()
        if not water_name:
            continue

        if water_name not in lookup:
            lookup[water_name] = {}

        lookup[water_name]["special_trout_water_lake"] = {
            "designation": record.get("Designatio", ""),
            "info": record.get("Info", ""),
            "tackle_regulation": record.get("Tackle_Reg", ""),
            "pro_regulation": record.get("Pro_Reg", ""),
            "trout_present": record.get("Trout_pres", ""),
            "acres": record.get("STW_acres", "")
        }

    # Process Special Trout Waters - Streams
    for record in regulations_data.get("special_trout_waters_streams", []):
        water_name = record.get("Water_Name", "").strip()
        if not water_name:
            continue

        if water_name not in lookup:
            lookup[water_name] = {}

        lookup[water_name]["special_trout_water_stream"] = {
            "designation": record.get("Designatio", ""),
            "info": record.get("Info", ""),
            "tackle_regulation": record.get("Tackle_Reg", ""),
            "pro_regulation": record.get("Pro_Reg", ""),
            "trout_present": record.get("Trout_pres", ""),
            "miles": record.get("STW_miles", "")
        }

    # Process Trophy Bass Waters
    for record in regulations_data.get("trophy_bass_waters", []):
        water_name = record.get("WaterName", "").strip()
        if not water_name:
            continue

        if water_name not in lookup:
            lookup[water_name] = {}

        lookup[water_name]["trophy_bass"] = {
            "info": record.get("Info", ""),
            "regulation": record.get("Regulation", "")
        }

    # Process Summer Catfish Waters
    for record in regulations_data.get("summer_catfish_waters", []):
        water_name = record.get("WaterName", "").strip()
        if not water_name:
            continue

        if water_name not in lookup:
            lookup[water_name] = {}

        lookup[water_name]["summer_catfish"] = {
            "info": record.get("Info", ""),
            "regulation": record.get("Regulation", "")
        }

    # Process Boat Ramps
    for record in regulations_data.get("boat_ramps", []):
        water_name = record.get("WaterName", "").strip()
        if not water_name:
            continue

        if water_name not in lookup:
            lookup[water_name] = {}

        if "boat_ramps" not in lookup[water_name]:
            lookup[water_name]["boat_ramps"] = []

        lookup[water_name]["boat_ramps"].append({
            "name": record.get("Name", ""),
            "info": record.get("Info", ""),
            "latitude": record.get("Latitude"),
            "longitude": record.get("Longitude")
        })

    return lookup


def main():
    """Main execution function."""

    # Fetch all regulation data
    regulations_data = fetch_all_regulations()

    # Build water body lookup table
    print("Building water body lookup table...")
    water_lookup = build_water_lookup(regulations_data)
    print(f"Created lookup for {len(water_lookup)} water bodies\n")

    # Save raw data
    raw_output_file = "regulations_raw.json"
    print(f"Saving raw data to {raw_output_file}...")
    with open(raw_output_file, 'w', encoding='utf-8') as f:
        json.dump(regulations_data, f, indent=2, ensure_ascii=False)
    print(f"Saved raw data\n")

    # Save lookup table
    lookup_output_file = "regulations_data.json"
    print(f"Saving lookup table to {lookup_output_file}...")

    output = {
        "waters": water_lookup,
        "metadata": regulations_data["metadata"]
    }

    with open(lookup_output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Saved lookup table\n")

    # Print summary
    print("="*60)
    print("Summary")
    print("="*60)
    print(f"Water bodies with regulations: {len(water_lookup)}")

    # Count by type
    counts = {
        "Special Trout Water (Lake)": 0,
        "Special Trout Water (Stream)": 0,
        "Trophy Bass": 0,
        "Summer Catfish": 0,
        "Has Boat Ramps": 0
    }

    for water_name, data in water_lookup.items():
        if "special_trout_water_lake" in data:
            counts["Special Trout Water (Lake)"] += 1
        if "special_trout_water_stream" in data:
            counts["Special Trout Water (Stream)"] += 1
        if "trophy_bass" in data:
            counts["Trophy Bass"] += 1
        if "summer_catfish" in data:
            counts["Summer Catfish"] += 1
        if "boat_ramps" in data:
            counts["Has Boat Ramps"] += 1

    for category, count in counts.items():
        print(f"  {category}: {count}")

    print("\nRegulation data fetch complete!")


if __name__ == "__main__":
    main()
