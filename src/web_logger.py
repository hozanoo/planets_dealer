# src/web_logger.py
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Set
from pathlib import Path
import time

CACHE_FILE = Path('data') / 'planet_type_cache.csv'

def load_cache(cache_path: Path) -> Dict[str, str]:
    """
    Loads the planet type cache from a CSV file.

    :param cache_path: The path to the cache CSV file.
    :type cache_path: Path
    :return: A dictionary mapping pl_name to planet_type.
    :rtype: Dict[str, str]
    """
    if cache_path.exists():
        try:
            df_cache = pd.read_csv(cache_path)
            if 'pl_name' in df_cache.columns and 'planet_type' in df_cache.columns:
                 cache_dict = pd.Series(df_cache.planet_type.values, index=df_cache.pl_name).astype(str).to_dict()
                 return {k: (v if v != 'nan' else 'Unknown') for k, v in cache_dict.items()}
            else:
                 print(f"Warnung: Cache-Datei {cache_path} fehlen Spalten 'pl_name'/'planet_type'. Starte neu.")
        except (pd.errors.EmptyDataError, pd.errors.ParserError, IOError, Exception) as e:
            print(f"Warnung: Cache-Datei {cache_path} konnte nicht gelesen werden. Starte neu. Fehler: {e}")
    return {}

def save_cache(cache_path: Path, cache_data: Dict[str, str]):
    """
    Saves the planet type cache to a CSV file.

    :param cache_path: The path to the cache CSV file.
    :type cache_path: Path
    :param cache_data: The dictionary (pl_name -> planet_type) to save.
    :type cache_data: Dict[str, str]
    """
    try:
        df_cache = pd.DataFrame(list(cache_data.items()), columns=['pl_name', 'planet_type'])
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df_cache.to_csv(cache_path, index=False, encoding='utf-8')
    except IOError as e:
        print(f"Warnung: Cache-Datei {cache_path} konnte nicht gespeichert werden. Fehler: {e}")
    except Exception as e:
        print(f"Warnung: Unerwarteter Fehler beim Speichern des Caches: {e}")

def get_nasa_planet_type(planet_name: str) -> str:
    """
    Fetches a planet's description from NASA and extracts its type.

    Scrapes the individual planet page on science.nasa.gov, searches for
    predefined keywords ('terrestrial', 'gas giant', etc.), and returns
    the matched type or 'Unknown'.

    :param planet_name: The name of the planet to scrape.
    :type planet_name: str
    :return: The found planet type (e.g., 'gas giant') or 'Unknown'.
    :rtype: str
    """
    planet_types: List[str] = ['Neptune-like', 'terrestrial', 'gas giant', 'super Earth']
    time.sleep(0.2)
    try:
        base_url: str = "https://science.nasa.gov/exoplanet-catalog/"
        if not isinstance(planet_name, str) or not planet_name:
             return 'Unknown'
        formatted_name: str = planet_name.lower().replace(" ", "-")
        url: str = f"{base_url}{formatted_name}/"
        headers: Dict[str, str] = {"User-Agent": "Mozilla/5.0 (compatible; MyExoplanetPipeline/1.0)"}
        res: requests.Response = requests.get(url, timeout=15, headers=headers)

        if res.status_code == 404:
            return 'Unknown'
        res.raise_for_status()

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
    return 'Unknown'

def add_planet_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches a DataFrame with a 'planet_type' column using CSV caching
    and sequential scraping with progress reporting.

    :param df: The DataFrame to enrich (must contain 'pl_name').
    :type df: pd.DataFrame
    :return: A new DataFrame with the 'planet_type' column added.
    :rtype: pd.DataFrame
    """
    if df.empty or 'pl_name' not in df.columns:
        print("Keine Daten oder 'pl_name'-Spalte zum Anreichern mit Planetentyp vorhanden.")
        return df

    print("Bestimme Planetentyp (mit CSV-Cache und sequenziellem Scraping)...")
    df_copy: pd.DataFrame = df.copy()
    df_copy['pl_name'] = df_copy['pl_name'].astype(str).fillna('')
    all_planet_names: Set[str] = set(df_copy['pl_name'].unique())
    if '' in all_planet_names: all_planet_names.remove('')

    planet_cache: Dict[str, str] = load_cache(CACHE_FILE)
    cached_planets: Set[str] = set(planet_cache.keys())
    missing_planets: List[str] = sorted(list(all_planet_names - cached_planets))

    newly_scraped_types: Dict[str, str] = {}
    if missing_planets:
        print(f"Cache nicht gefunden für {len(missing_planets)} Planeten. Starte sequenzielles Scraping...")
        scraped_count = 0
        total_missing = len(missing_planets)

        for i, planet_name in enumerate(missing_planets):
            planet_type = get_nasa_planet_type(planet_name)
            newly_scraped_types[planet_name] = planet_type
            scraped_count += 1

            if scraped_count % 10 == 0 or scraped_count == total_missing:
                print(f" Fortschritt Scraping: {scraped_count}/{total_missing} Planeten verarbeitet...")

        found_new_count = len([v for v in newly_scraped_types.values() if v != 'Unknown'])
        print(f"Scraping abgeschlossen. {found_new_count} neue Typen gefunden (von {total_missing} Versuchen).")

        planet_cache.update(newly_scraped_types)
        save_cache(CACHE_FILE, planet_cache)
    else:
        print("Cache-Treffer für alle Planeten. Kein Scraping notwendig.")

    df_copy['planet_type'] = df_copy['pl_name'].map(planet_cache).fillna('Unknown')
    found_count = len(df_copy[df_copy['planet_type'] != 'Unknown'])
    print(f"Planetentyp-Spalte gefüllt ({found_count} bekannte Typen).")

    return df_copy