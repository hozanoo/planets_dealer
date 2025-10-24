import requests
import pandas as pd
import io
import re
from typing import Dict, Any, Optional

API_URL: str = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

def fetch_exoplanets(limit: Optional[int] = 100) -> pd.DataFrame:
    """Fetches system and planet data from PSCompPars."""
    top_clause = f"TOP {limit}" if limit and limit > 0 else ""
    query: str = f"""
        SELECT {top_clause}
            hostname, pl_name, disc_year,
            pl_orbper, pl_orbsmax, pl_rade, pl_masse,
            pl_orbeccen, pl_eqt, pl_insol
        FROM PSCompPars
    """
    query = re.sub(r'\s+', ' ', query).strip()
    params: Dict[str, Any] = {"query": query, "format": "csv"}
    print(f"Fetching System & Planet data (PSCompPars) from NASA API{' with limit ' + str(limit) if limit and limit > 0 else ' (all data)'}...")
    try:
        timeout_seconds = 60 if limit and limit > 0 else 300
        r: requests.Response = requests.get(API_URL, params=params, timeout=timeout_seconds)
        r.raise_for_status()
        if not r.text.strip():
            print("Warning: Empty response from NASA API (PSCompPars).")
            return pd.DataFrame()
        df: pd.DataFrame = pd.read_csv(io.StringIO(r.text))
        print(f"{len(df)} rows fetched from PSCompPars API.")
        rename_map = {
            'hostname': 'star_name_api', 'disc_year': 'discovery_year',
            'pl_orbper': 'orbital_period_days', 'pl_orbsmax': 'orbit_semi_major_axis_au',
            'pl_rade': 'planet_radius_earth_radii', 'pl_masse': 'planet_mass_earth_masses',
            'pl_orbeccen': 'orbit_eccentricity', 'pl_eqt': 'equilibrium_temperature_k',
            'pl_insol': 'insolation_flux_earth_flux'
        }
        df.rename(columns=rename_map, inplace=True)
        return df
    except Exception as e:
        print(f"Error fetching NASA data (PSCompPars): {e}")
        return pd.DataFrame()

def fetch_stellar_hosts() -> pd.DataFrame:
    """Fetches aggregated star data from 'stellarhosts' via GROUP BY."""
    query: str = """
        SELECT
            sy_name, hostname,
            AVG(st_teff) as st_teff, AVG(st_lum) as st_lum,
            AVG(st_age) as st_age, AVG(st_met) as st_met
        FROM stellarhosts
        GROUP BY sy_name, hostname
    """
    params: Dict[str, Any] = {"query": query, "format": "csv"}
    print("Fetching aggregated star data (stellarhosts via GROUP BY) from NASA API...")
    try:
        r: requests.Response = requests.get(API_URL, params=params, timeout=120)
        r.raise_for_status()
        if not r.text.strip():
            print("Warning: Empty response from NASA API (stellarhosts).")
            return pd.DataFrame()
        df: pd.DataFrame = pd.read_csv(io.StringIO(r.text))
        df_renamed: pd.DataFrame = df.rename(columns={'sy_name': 'system_key', 'hostname': 'star_name'})
        print(f"{len(df_renamed)} unique stars fetched from stellarhosts API.")
        return df_renamed
    except Exception as e:
        print(f"Error fetching stellarhosts data: {e}")
        return pd.DataFrame()