#!/usr/bin/env python3
"""
Match regulation data from ArcGIS with stocking data water bodies.

This script:
1. Loads stocking_data.json (168 water bodies)
2. Loads regulations_data.json (43 water bodies with regulations)
3. Matches water names using fuzzy matching
4. Creates a combined dataset for the website
"""

import json
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Any


def load_json(filename: str) -> Dict:
    """Load JSON file."""
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_name(name: str) -> str:
    """
    Normalize water body name for better matching.

    Args:
        name: Water body name

    Returns:
        Normalized name
    """
    if not name:
        return ""

    # Convert to lowercase
    name = name.lower().strip()

    # Common replacements for consistency
    replacements = {
        " reservoir": " res",
        " hatchery": " hatch",
        " wildlife": " wl",
        " management": " mgmt",
        " recreation": " rec",
        "'s": "s",
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    return name


def similarity_ratio(str1: str, str2: str) -> float:
    """
    Calculate similarity ratio between two strings.

    Args:
        str1: First string
        str2: Second string

    Returns:
        Similarity ratio (0.0 to 1.0)
    """
    return SequenceMatcher(None, normalize_name(str1), normalize_name(str2)).ratio()


def find_best_match(target_name: str, candidate_names: List[str], threshold: float = 0.85) -> Tuple[str, float]:
    """
    Find the best matching name from candidates.

    Args:
        target_name: Name to match
        candidate_names: List of candidate names
        threshold: Minimum similarity threshold

    Returns:
        Tuple of (best_match_name, similarity_score) or (None, 0.0) if no match
    """
    best_match = None
    best_score = 0.0

    for candidate in candidate_names:
        score = similarity_ratio(target_name, candidate)
        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= threshold:
        return best_match, best_score
    else:
        return None, 0.0


def match_regulations_to_stocking(stocking_data: Dict, regulations_data: Dict) -> Dict[str, Any]:
    """
    Match regulation data to stocking data water bodies.

    Args:
        stocking_data: Dict of water bodies with stocking records
        regulations_data: Dict of water bodies with regulations

    Returns:
        Dict mapping stocking water names to their regulations (if found)
    """
    matched = {}
    unmatched_stocking = []
    unmatched_regulations = list(regulations_data["waters"].keys())

    stocking_water_names = list(stocking_data.keys())
    regulation_water_names = list(regulations_data["waters"].keys())

    print(f"\nMatching {len(stocking_water_names)} stocking waters with {len(regulation_water_names)} regulation waters...\n")

    # Try to match each stocking water body
    for stocking_name in stocking_water_names:
        if stocking_name in NO_AUTO_MATCH:
            unmatched_stocking.append(stocking_name)
            continue
        match_name, score = find_best_match(stocking_name, regulation_water_names, threshold=0.75)

        if match_name:
            matched[stocking_name] = {
                "regulations": regulations_data["waters"][match_name],
                "match_score": score,
                "regulation_water_name": match_name
            }
            if match_name in unmatched_regulations:
                unmatched_regulations.remove(match_name)
            print(f"  MATCH: '{stocking_name}' -> '{match_name}' (score: {score:.2f})")
        else:
            unmatched_stocking.append(stocking_name)

    # Print summary
    print(f"\n{'='*70}")
    print("Matching Summary")
    print(f"{'='*70}")
    print(f"Total stocking waters: {len(stocking_water_names)}")
    print(f"Total regulation waters: {len(regulation_water_names)}")
    print(f"Successful matches: {len(matched)}")
    print(f"Unmatched stocking waters: {len(unmatched_stocking)}")
    print(f"Unmatched regulation waters: {len(unmatched_regulations)}")

    # Show unmatched regulations (these might be in stocking data with different names)
    if unmatched_regulations:
        print(f"\n{'='*70}")
        print("Unmatched Regulation Waters (may need manual mapping):")
        print(f"{'='*70}")
        for name in sorted(unmatched_regulations):
            # Try to find close matches
            close_matches = []
            for stocking_name in stocking_water_names:
                score = similarity_ratio(name, stocking_name)
                if 0.5 <= score < 0.75:  # Close but not close enough
                    close_matches.append((stocking_name, score))

            close_matches.sort(key=lambda x: x[1], reverse=True)

            print(f"\n  '{name}'")
            if close_matches[:3]:
                print("    Possible matches:")
                for match_name, score in close_matches[:3]:
                    print(f"      - '{match_name}' (score: {score:.2f})")

    return matched


# Stocking waters whose closest regulation name is a DIFFERENT water. Never auto-match these.
NO_AUTO_MATCH = {
    "Peralta Creek",        # would match Mineral Creek
    "Trees Lake",           # would match Ute Lake
    "Lost Lake",            # would match Alto Lake
    "Tajique Creek",        # would match Tanques Creek
    "Rito De Los Pinos",    # would match Rio de Los Pinos (a different river)
    "Cebolla River",        # Rio Arriba water; the Rio Cebolla STW is the Jemez one
    "Hondo River (Lower)",  # would match South Fork Rio Hondo
}


def create_manual_mapping() -> Dict[str, str]:
    """
    Create manual mappings for waters that don't match automatically.

    Returns:
        Dict mapping stocking water names to regulation water names
    """
    # Manual mappings based on close matches and domain knowledge
    manual_mappings = {
        # Fix incorrect auto-match (San Juan River != San Antonio River)
        "San Antonio River": "Rio San Antonio",

        # Common river/stream name variations
        "Costilla River": "Rio Costilla",
        "Costilla Creek": "Rio Costilla",
        "Ruidoso River": "Rio Ruidoso",
        "Guadalupe River": "Rio Guadalupe",
        "Los Pinos River": "Rio de Los Pinos",
        "Rio Las Vacas": "Rio del Las Vacas",

        # Multi-reach rivers: every reach is listed with its own description
        "San Juan River (Quality)": "San Juan River",
        "Cimarron River (West Of Cimarron)": "Cimarron River",
        "Red River (Abv Questa)": "Red River",
        "Red River (Below Questa)": "Red River",
        "Chama River (Blw El Vado)": "Rio Chama",
        "Chama River (Below Abiquiu)": "Rio Chama",
        "Rio Grande (Gorge- Abv Pilar)": "Rio Grande",
        "Gilita Creek": "Gilita Creek and Willow Creek",
        "Willow Creek (Gila Drainage)": "Gilita Creek and Willow Creek",
        "Comanche Creek (Upper)": "Valle Vidal",
        "Comanche Creek (Lower)": "Valle Vidal",
        "Shuree Ponds": "Valle Vidal",

        # Lakes / ponds with different official names
        "Heron Reservoir": "Heron Lake",
        "Lake Sumner": "Sumner Lake",
        "Carlsbad Municipal Lake": "Lake Carlsbad",
        "Perch Lake (Guadalupe County)": "Perch Lake",
        "Conoco Pond": "Conoco Lake",
        "Eunice Lake": "Eunice Pond",
        "Grants Municipal Pond (River Walk Pond)": "Grants Riverwalk Pond",
        "Oasis Park Lake": "Oasis State Park Pond",
        "Roswell Kids Pond": "Roswell Kid's Pond (Spring River Park)",
        "Conservancy Park Lake (Aka Tingley Beach)": "Tingley Beach Ponds",
    }
    return manual_mappings


def main():
    """Main execution function."""

    # Load data files
    print("Loading data files...")
    stocking_data = load_json("stocking_data.json")
    # Waters that get a page without stocking records (extra_waters.json) also get regulations.
    import os
    if os.path.exists("extra_waters.json"):
        for name in load_json("extra_waters.json"):
            if not name.startswith("_"):
                stocking_data.setdefault(name, {"records": []})
    regulations_data = load_json("regulations_data.json")
    print(f"  Loaded {len(stocking_data)} water bodies from stocking_data.json")
    print(f"  Loaded {len(regulations_data['waters'])} water bodies from regulations_data.json")

    # Match regulations to stocking waters
    matched = match_regulations_to_stocking(stocking_data, regulations_data)

    # Apply manual mappings
    manual_mappings = create_manual_mapping()
    if manual_mappings:
        print(f"\nApplying {len(manual_mappings)} manual mappings...")
        for stocking_name, regulation_name in manual_mappings.items():
            if stocking_name not in stocking_data:
                print(f"  SKIP manual mapping: '{stocking_name}' is not a stocking water")
                continue
            if regulation_name in regulations_data["waters"]:
                matched[stocking_name] = {
                    "regulations": regulations_data["waters"][regulation_name],
                    "match_score": 1.0,  # Manual match
                    "regulation_water_name": regulation_name,
                    "manual_mapping": True
                }
                print(f"  MANUAL: '{stocking_name}' -> '{regulation_name}'")

    # Save matched data
    output = {
        "matched_waters": matched,
        "metadata": {
            "total_stocking_waters": len(stocking_data),
            "total_regulation_waters": len(regulations_data["waters"]),
            "matched_count": len(matched),
            "match_rate": f"{len(matched) / len(stocking_data) * 100:.1f}%",
            "regulation_metadata": regulations_data["metadata"]
        }
    }

    output_file = "matched_regulations.json"
    print(f"\nSaving matched data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Saved matched data")

    print("\nMatching complete!")
    print(f"Match rate: {len(matched)}/{len(stocking_data)} ({len(matched) / len(stocking_data) * 100:.1f}%)")


if __name__ == "__main__":
    main()
