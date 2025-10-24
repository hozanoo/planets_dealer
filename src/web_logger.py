# src/web_logger.py
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Set
from pathlib import Path
import time # For delays

# Path to the cache file (CSV)
CACHE_FILE = Path('data') / 'planet_type_cache.csv'

def load_cache(cache_path: Path) -> Dict[str, str]:
    """Loads the planet type cache from a CSV file."""
    if cache_path.exists():
        try:
            df_cache = pd.read_csv(cache_path)
            # Ensure expected columns exist
            if 'pl_name' in df_cache.columns and 'planet_type' in df_cache.columns:
                 # Create dictionary, handle NaN/None in planet_type
                 cache_dict = pd.Series(df_cache.planet_type.values, index=df_cache.pl_name).astype(str).to_dict()
                 # Convert 'nan' string back to 'Unknown'
                 return {k: (v if v != 'nan' else 'Unknown') for k, v in cache_dict.items()}
            else:
                 print(f"Warnung: Cache-Datei {cache_path} fehlen Spalten 'pl_name'/'planet_type'. Starte neu.")
        except (pd.errors.EmptyDataError, pd.errors.ParserError, IOError, Exception) as e:
            print(f"Warnung: Cache-Datei {cache_path} konnte nicht gelesen werden. Starte neu. Fehler: {e}")
    # Return empty dict if cache doesn't exist or is faulty
    return {}

def save_cache(cache_path: Path, cache_data: Dict[str, str]):
    """Saves the planet type cache to a CSV file."""
    try:
        # Create DataFrame from dictionary
        df_cache = pd.DataFrame(list(cache_data.items()), columns=['pl_name', 'planet_type'])
        # Create data directory if needed
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        # Save as CSV
        df_cache.to_csv(cache_path, index=False, encoding='utf-8')
    except IOError as e:
        print(f"Warnung: Cache-Datei {cache_path} konnte nicht gespeichert werden. Fehler: {e}")
    except Exception as e:
        print(f"Warnung: Unerwarteter Fehler beim Speichern des Caches: {e}")


def get_nasa_planet_type(planet_name: str) -> str:
    """Fetches description from NASA and extracts the planet type."""
    planet_types: List[str] = ['Neptune-like', 'terrestrial', 'gas giant', 'super Earth']
    # Small delay between requests
    time.sleep(0.2) # 0.2 second delay
    try:
        base_url: str = "https://science.nasa.gov/exoplanet-catalog/"
        if not isinstance(planet_name, str) or not planet_name:
             # print(f"Warnung: Ungültiger Planetenname übersprungen: {planet_name}") # Keep commented out
             return 'Unknown'
        formatted_name: str = planet_name.lower().replace(" ", "-")
        url: str = f"{base_url}{formatted_name}/"
        headers: Dict[str, str] = {"User-Agent": "Mozilla/5.0 (compatible; MyExoplanetPipeline/1.0)"}
        res: requests.Response = requests.get(url, timeout=15, headers=headers)

        if res.status_code == 404:
            return 'Unknown'
        res.raise_for_status() # Raise error for 5xx etc.

        soup: BeautifulSoup = BeautifulSoup(res.content, "html.parser")
        desc_element = soup.select_one("div.custom-field span")

        if desc_element:
            description_text: str = desc_element.get_text(strip=True).lower()
            for p_type in planet_types:
                if p_type.lower() in description_text:
                    original_type: str = next(t for t in planet_types if t.lower() == p_type.lower())
                    return original_type
    except requests.exceptions.Timeout:
        print(f"Warnung: Timeout beim Scrapen von {planet_name}.")
    except requests.exceptions.RequestException as e:
        print(f"Warnung: Anfragefehler beim Scrapen von {planet_name}: {e}")
    except Exception as e:
        print(f"Warnung: Unerwarteter Fehler beim Scrapen von {planet_name}: {e}")
    # Default value if description not found, type not matched, or error occurred
    return 'Unknown'

def add_planet_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the 'planet_type' column using CSV caching and sequential scraping
    with progress reporting.
    """
    if df.empty or 'pl_name' not in df.columns:
        print("Keine Daten oder 'pl_name'-Spalte zum Anreichern mit Planetentyp vorhanden.")
        return df

    print("Bestimme Planetentyp (mit CSV-Cache und sequenziellem Scraping)...")
    df_copy: pd.DataFrame = df.copy()
    # Ensure pl_name is string and handle NaNs
    df_copy['pl_name'] = df_copy['pl_name'].astype(str).fillna('')
    all_planet_names: Set[str] = set(df_copy['pl_name'].unique())
    # Remove empty string if present
    if '' in all_planet_names: all_planet_names.remove('')

    # 1. Load cache
    planet_cache: Dict[str, str] = load_cache(CACHE_FILE)
    cached_planets: Set[str] = set(planet_cache.keys())

    # 2. Identify missing planets
    missing_planets: List[str] = sorted(list(all_planet_names - cached_planets))

    # 3. Scrape missing planets SEQUENTIALLY (with progress)
    newly_scraped_types: Dict[str, str] = {}
    if missing_planets:
        print(f"Cache nicht gefunden für {len(missing_planets)} Planeten. Starte sequenzielles Scraping...")
        scraped_count = 0
        total_missing = len(missing_planets)

        for i, planet_name in enumerate(missing_planets):
            # Get type for the missing planet
            planet_type = get_nasa_planet_type(planet_name)
            newly_scraped_types[planet_name] = planet_type
            scraped_count += 1

            # Report progress every 10 planets or at the end
            if scraped_count % 10 == 0 or scraped_count == total_missing:
                print(f" Fortschritt Scraping: {scraped_count}/{total_missing} Planeten verarbeitet...")

        # Count how many new types (not 'Unknown') were found
        found_new_count = len([v for v in newly_scraped_types.values() if v != 'Unknown'])
        print(f"Scraping abgeschlossen. {found_new_count} neue Typen gefunden (von {total_missing} Versuchen).")

        # 4. Update cache (in memory)
        planet_cache.update(newly_scraped_types)

        # 5. Save cache (to disk)
        save_cache(CACHE_FILE, planet_cache)
    else:
        # If no planets were missing
        print("Cache-Treffer für alle Planeten. Kein Scraping notwendig.")

    # 6. Apply types from cache to DataFrame
    # .map() is efficient for mapping values from a dictionary
    # .fillna('Unknown') handles cases if a name is somehow missing
    df_copy['planet_type'] = df_copy['pl_name'].map(planet_cache).fillna('Unknown')

    # Count how many planets now have a known type
    found_count = len(df_copy[df_copy['planet_type'] != 'Unknown'])
    print(f"Planetentyp-Spalte gefüllt ({found_count} bekannte Typen).")

    # Return the DataFrame with the new column
    return df_copy