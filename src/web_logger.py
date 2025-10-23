import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict

def get_nasa_planet_type(planet_name: str) -> str:
    """Fetches description from NASA and extracts planet type."""
    planet_types: List[str] = ['Neptune-like', 'terrestrial', 'gas giant', 'super Earth']
    try:
        base_url: str = "https://science.nasa.gov/exoplanet-catalog/"
        formatted_name: str = planet_name.lower().replace(" ", "-")
        url: str = f"{base_url}{formatted_name}/"
        headers: Dict[str, str] = {"User-Agent": "Mozilla/5.0"}
        res: requests.Response = requests.get(url, timeout=10, headers=headers)
        if res.status_code == 404: return 'Unknown'
        soup: BeautifulSoup = BeautifulSoup(res.content, "html.parser")
        desc_element = soup.select_one("div.custom-field span")
        if desc_element:
            description_text: str = desc_element.get_text(strip=True).lower()
            for p_type in planet_types:
                if p_type.lower() in description_text:
                    original_type: str = next(t for t in planet_types if t.lower() == p_type.lower())
                    return original_type
    except Exception: pass
    return 'Unknown'

def add_planet_type(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the 'planet_type' column via scraping."""
    if df.empty or 'pl_name' not in df.columns:
        print("No data or 'pl_name' column to enrich with planet type.")
        return df
    print("Determining planet type (NASA Scraping)... (This may take a while!)")
    df_copy: pd.DataFrame = df.copy()
    df_copy['planet_type'] = df_copy['pl_name'].apply(get_nasa_planet_type)
    found_count = len(df_copy[df_copy['planet_type'] != 'Unknown'])
    print(f"Planet types added ({found_count} found).")
    return df_copy