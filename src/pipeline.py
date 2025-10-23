import os
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import io
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import UndefinedTable
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any, Optional

# Load environment variables
BASE_DIR: Path = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')


# ==============================================================
# 1. EXTRACT (API - SYSTEMS & PLANETS)
# ==============================================================
def fetch_exoplanets(limit: Optional[int] = 100) -> pd.DataFrame:
    """Fetches system and planet data from PSCompPars."""
    api_url: str = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    top_clause = f"TOP {limit}" if limit and limit > 0 else ""
    query: str = f"""
        SELECT {top_clause}
            hostname, pl_name, disc_year,
            pl_orbper, pl_orbsmax, pl_rade, pl_masse,
            pl_orbeccen, pl_eqt, pl_insol
        FROM PSCompPars
        WHERE sy_snum > 1
    """
    query = re.sub(r'\s+', ' ', query).strip()
    params: Dict[str, Any] = {"query": query, "format": "csv"}
    print(f"Fetching System & Planet data (PSCompPars) from NASA API{' with limit ' + str(limit) if limit and limit > 0 else ' (all data)'}...")
    try:
        timeout_seconds = 60 if limit and limit > 0 else 300
        r: requests.Response = requests.get(api_url, params=params, timeout=timeout_seconds)
        r.raise_for_status()
        if not r.text.strip():
            print("Warning: Empty response from NASA API (PSCompPars).")
            return pd.DataFrame()
        df: pd.DataFrame = pd.read_csv(io.StringIO(r.text))
        print(f"{len(df)} rows fetched from PSCompPars API.")
        # Rename columns to final English names immediately
        rename_map = {
            'hostname': 'star_name_api', # Temporary name to avoid clash before merge
            'disc_year': 'discovery_year',
            'pl_orbper': 'orbital_period_days',
            'pl_orbsmax': 'orbit_semi_major_axis_au',
            'pl_rade': 'planet_radius_earth_radii',
            'pl_masse': 'planet_mass_earth_masses',
            'pl_orbeccen': 'orbit_eccentricity',
            'pl_eqt': 'equilibrium_temperature_k',
            'pl_insol': 'insolation_flux_earth_flux'
        }
        df.rename(columns=rename_map, inplace=True)
        return df
    except Exception as e:
        print(f"Error fetching NASA data (PSCompPars): {e}")
        return pd.DataFrame()

# ==============================================================
# 2. EXTRACT (LOCAL CSV 'hwc.csv')
# ==============================================================
def load_local_data(csv_path: Path) -> pd.DataFrame:
    """Loads supplementary data from the local 'hwc.csv'."""
    print(f"Loading supplementary data from '{csv_path}'...")
    try:
        df: pd.DataFrame = pd.read_csv(csv_path)
        needed_cols_mapping: Dict[str, str] = {
            # Original CSV Name : Final English DataFrame Name
            "P_NAME": "pl_name_local", # Used for merging
            "P_ESI": "esi",
            "P_DETECTION": "detection_method_name", # For lookup table
            "P_DISCOVERY_FACILITY": "facility_name", # For lookup table
            "S_CONSTELLATION_ENG": "constellation_en",
            "S_DISTANCE": "distance_pc",
            "S_HZ_CON_MIN": "hz_conservative_inner_au",
            "S_HZ_CON_MAX": "hz_conservative_outer_au",
            "S_HZ_OPT_MIN": "hz_optimistic_inner_au",
            "S_HZ_OPT_MAX": "hz_optimistic_outer_au"
        }
        original_needed_cols = list(needed_cols_mapping.keys())
        all_cols_in_df: List[str] = df.columns.tolist()
        missing_cols: List[str] = [col for col in original_needed_cols if col not in all_cols_in_df]
        if missing_cols:
             raise KeyError(f"Missing columns in '{csv_path.name}': {missing_cols}.")

        df_renamed: pd.DataFrame = df[original_needed_cols].rename(columns=needed_cols_mapping)
        print(f"{len(df_renamed)} rows loaded from local CSV.")
        return df_renamed
    except FileNotFoundError:
        print(f"ERROR: File '{csv_path}' not found.")
        return pd.DataFrame()
    except (KeyError, Exception) as e:
        print(f"ERROR reading CSV '{csv_path}': {e}")
        return pd.DataFrame()

# ==============================================================
# 2.5 EXTRACT (API - STARS via GROUP BY)
# ==============================================================
def fetch_stellar_hosts() -> pd.DataFrame:
    """Fetches aggregated star data from 'stellarhosts' via GROUP BY."""
    api_url: str = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
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
        r: requests.Response = requests.get(api_url, params=params, timeout=120)
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

# ==============================================================
# 3. TRANSFORM (SCRAPING - Planet Type)
# ==============================================================
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
    except Exception: pass # Ignore scraping errors, return Unknown
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

# ==============================================================
# 4. LOAD (POSTGRESQL - 5 Tables)
# ==============================================================
class DatabaseError(Exception): pass

class ExoplanetDBPostgres:
    def __init__(self):
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        try:
            self.connection = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'), host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT', 5432)
            )
            self.cursor = self.connection.cursor()
            print(f"Successfully connected to DB '{os.environ.get('DB_NAME')}' on host '{os.environ.get('DB_HOST')}'.")
        except Exception as e:
            raise DatabaseError(f"Database connection failed: {e}")

    def _execute_values(self, query: str, data_tuples: List[Tuple], page_size: int = 100):
        if not data_tuples: return
        if not self.cursor: raise DatabaseError("Cursor not available.")
        try:
            execute_values(self.cursor, query, data_tuples, page_size=page_size)
        except Exception as e:
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Error executing bulk insert: {e}\nQuery Template: {query[:500]}...")

    def _recreate_tables(self):
        """Creates the 5-table schema including ENUM and generated column."""
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        try:
            print("Resetting table schema (DROP/CREATE)...")
            self.cursor.execute("DROP TABLE IF EXISTS planets CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS stars CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS systems CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS detection_methods CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS discovery_facilities CASCADE;")
            self.cursor.execute("DROP TYPE IF EXISTS planet_type_enum CASCADE;")

            self.cursor.execute("CREATE TYPE planet_type_enum AS ENUM ('Neptune-like', 'terrestrial', 'gas giant', 'super Earth', 'Unknown');")
            self.cursor.execute("CREATE TABLE discovery_facilities (facility_id SERIAL PRIMARY KEY, facility_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute("CREATE TABLE detection_methods (method_id SERIAL PRIMARY KEY, method_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute("""
                CREATE TABLE systems (
                    system_key TEXT PRIMARY KEY,
                    constellation_en TEXT, distance_pc REAL,
                    hz_conservative_inner_au REAL, hz_conservative_outer_au REAL,
                    hz_optimistic_inner_au REAL, hz_optimistic_outer_au REAL
                );""")
            self.cursor.execute("""
                CREATE TABLE stars (
                    star_name TEXT PRIMARY KEY,
                    system_key TEXT REFERENCES systems(system_key) ON DELETE SET NULL,
                    st_teff REAL, st_lum REAL, st_age REAL, st_met REAL
                );""")
            self.cursor.execute("""
                CREATE TABLE planets (
                    pl_name TEXT PRIMARY KEY,
                    star_name TEXT REFERENCES stars(star_name) ON DELETE SET NULL,
                    method_id INTEGER REFERENCES detection_methods(method_id) ON DELETE SET NULL,
                    facility_id INTEGER REFERENCES discovery_facilities(facility_id) ON DELETE SET NULL,
                    planet_type planet_type_enum,
                    discovery_year INTEGER, orbital_period_days REAL, orbit_semi_major_axis_au REAL,
                    planet_radius_earth_radii REAL, planet_mass_earth_masses REAL, orbit_eccentricity REAL,
                    equilibrium_temperature_k REAL, insolation_flux_earth_flux REAL, esi REAL,
                    visualization_url TEXT GENERATED ALWAYS AS (
                        'https://eyes.nasa.gov/apps/exo/#/planet/' || replace(pl_name, ' ', '_')
                    ) STORED
                );""")
            self.connection.commit()
            print("Tables (including lookups, ENUM, generated column) created successfully.")
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Error creating tables: {e}")

    def _dataframe_to_tuples(self, df: pd.DataFrame) -> List[Tuple]:
        """Converts DataFrame to tuples (handles NaN/NA)."""
        df_clean: pd.DataFrame = df.astype(object).where(pd.notnull(df), None)
        df_clean = df_clean.replace({pd.NA: None})
        return [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

    def _insert_lookup_data(self, df_merged: pd.DataFrame) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Inserts data into lookup tables and returns mapping dicts."""
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        method_map: Dict[str, int] = {}; facility_map: Dict[str, int] = {}
        try:
            # Detection Methods
            if 'detection_method_name' in df_merged.columns:
                unique_methods = df_merged['detection_method_name'].dropna().unique()
                method_tuples: List[Tuple] = [(name,) for name in unique_methods]
                if method_tuples:
                    query = "INSERT INTO detection_methods (method_name) VALUES (%s) ON CONFLICT (method_name) DO NOTHING;"
                    self.cursor.executemany(query, method_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT method_name, method_id FROM detection_methods")
                    method_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(method_map)} detection methods inserted/found.")

            # Discovery Facilities
            if 'facility_name' in df_merged.columns:
                unique_facilities = df_merged['facility_name'].dropna().unique()
                facility_tuples: List[Tuple] = [(name,) for name in unique_facilities]
                if facility_tuples:
                    query = "INSERT INTO discovery_facilities (facility_name) VALUES (%s) ON CONFLICT (facility_name) DO NOTHING;"
                    self.cursor.executemany(query, facility_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT facility_name, facility_id FROM discovery_facilities")
                    facility_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(facility_map)} discovery facilities inserted/found.")

            return method_map, facility_map
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Error inserting lookup data: {e}")

    def insert_data(self, df_main_merged: pd.DataFrame, df_stars_api: pd.DataFrame):
        """Splits DataFrames and inserts into the 5 tables."""
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        if df_main_merged.empty or df_stars_api.empty:
            print("Warning: No data to insert (one source might be empty).")
            return
        try:
            self._recreate_tables()
            method_map, facility_map = self._insert_lookup_data(df_main_merged)

            # --- Systems ---
            print("Preparing systems data...")
            df_systems: pd.DataFrame = df_main_merged.rename(columns={'star_name_api': 'system_key'}) # Use temp API name
            system_cols: List[str] = [
                'system_key', 'constellation_en', 'distance_pc',
                'hz_conservative_inner_au', 'hz_conservative_outer_au',
                'hz_optimistic_inner_au', 'hz_optimistic_outer_au'
            ]
            valid_system_cols: List[str] = [col for col in system_cols if col in df_systems.columns]
            df_systems = df_systems[valid_system_cols].drop_duplicates(subset=['system_key']).dropna(subset=['system_key'])
            system_tuples: List[Tuple] = self._dataframe_to_tuples(df_systems)

            # --- Stars ---
            print("Preparing stars data...")
            star_cols: List[str] = ['star_name', 'system_key', 'st_teff', 'st_lum', 'st_age', 'st_met']
            valid_star_cols = [col for col in star_cols if col in df_stars_api.columns]
            df_stars: pd.DataFrame = df_stars_api[valid_star_cols].copy()
            df_stars = df_stars[df_stars['system_key'].isin(df_systems['system_key'])] # Ensure stars belong to fetched systems
            df_stars = df_stars.drop_duplicates(subset=['star_name']).dropna(subset=['star_name'])
            star_tuples: List[Tuple] = self._dataframe_to_tuples(df_stars)

            # --- Planets ---
            print("Preparing planets data...")
            df_planets_prep: pd.DataFrame = df_main_merged.rename(columns={'star_name_api': 'star_name'}) # Final rename
            df_planets_prep['method_id'] = df_planets_prep['detection_method_name'].map(method_map) if 'detection_method_name' in df_planets_prep.columns else None
            df_planets_prep['facility_id'] = df_planets_prep['facility_name'].map(facility_map) if 'facility_name' in df_planets_prep.columns else None
            planet_cols: List[str] = [ # Order matches CREATE TABLE (excluding generated visualization_url)
                'pl_name', 'star_name', 'method_id', 'facility_id', 'planet_type',
                'discovery_year', 'orbital_period_days', 'orbit_semi_major_axis_au',
                'planet_radius_earth_radii', 'planet_mass_earth_masses', 'orbit_eccentricity',
                'equilibrium_temperature_k', 'insolation_flux_earth_flux', 'esi'
            ]
            valid_planet_cols: List[str] = [col for col in planet_cols if col in df_planets_prep.columns]
            df_planets: pd.DataFrame = df_planets_prep[valid_planet_cols]
            df_planets = df_planets.drop_duplicates(subset=['pl_name']).dropna(subset=['pl_name'])
            df_planets = df_planets[df_planets['star_name'].isin(df_stars['star_name'])] # Ensure planets belong to fetched stars
            planet_tuples: List[Tuple] = self._dataframe_to_tuples(df_planets)

            # --- Insert ---
            print(f"Inserting {len(system_tuples)} systems...")
            cols_systems_str: str = ", ".join(valid_system_cols)
            query_systems: str = f"INSERT INTO systems ({cols_systems_str}) VALUES %s ON CONFLICT (system_key) DO NOTHING"
            self._execute_values(query_systems, system_tuples)

            print(f"Inserting {len(star_tuples)} stars...")
            cols_stars_str: str = ", ".join(valid_star_cols)
            query_stars: str = f"INSERT INTO stars ({cols_stars_str}) VALUES %s ON CONFLICT (star_name) DO NOTHING"
            self._execute_values(query_stars, star_tuples)

            print(f"Inserting {len(planet_tuples)} planets...")
            cols_planets_str: str = ", ".join(valid_planet_cols)
            query_planets: str = f"INSERT INTO planets ({cols_planets_str}) VALUES %s ON CONFLICT (pl_name) DO NOTHING"
            self._execute_values(query_planets, planet_tuples)

            self.connection.commit()
            print("Data successfully inserted into all tables.")
        except Exception as e:
            print(f"ERROR during data insertion: {e}")
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Transaction failed: {e}")

    def close_connection(self):
        try:
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing DB connection: {e}")
        finally:
            self.cursor = None; self.connection = None

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close_connection()

def save_normalized_to_db(df_main_merged: pd.DataFrame, df_stars_api: pd.DataFrame):
    """Saves the DataFrames into five normalized tables (3NF)."""
    try:
        with ExoplanetDBPostgres() as db:
            db.insert_data(df_main_merged, df_stars_api)
    except (DatabaseError, ValueError, Exception) as e:
        # Catch generic Exception as well for broader safety
        print(f"Error saving data to database: {e}")
        raise # Re-raise the exception to signal failure

# ==============================================================
# 5. ORCHESTRATION (MAIN)
# ==============================================================
def run_pipeline(limit: Optional[int] = 100):
    """Executes the entire ETL pipeline."""
    print(f"Starting Exoplanet ETL Pipeline (5 tables) with limit={limit if limit else 'None'}...")

    # 1. EXTRACT
    df_nasa: pd.DataFrame = fetch_exoplanets(limit=limit)
    if df_nasa.empty:
        print("Pipeline stopped: No primary data fetched from NASA API.")
        return # Stop if essential data is missing

    local_csv_path: Path = BASE_DIR / 'data' / 'hwc.csv'
    df_local: pd.DataFrame = load_local_data(local_csv_path)
    if df_local.empty:
        print("Warning: Local supplementary data could not be loaded or is empty. Proceeding without it.")

    df_stars_api: pd.DataFrame = fetch_stellar_hosts()
    if df_stars_api.empty:
        print("Pipeline stopped: No star data fetched from API.")
        return # Stop if essential star data is missing

    # 2. TRANSFORM (MERGE - API + Local CSV)
    # Ensure pl_name exists and create normalized key
    if 'pl_name' not in df_nasa.columns:
        print("ERROR: 'pl_name' column missing from NASA API data. Cannot merge.")
        return
    df_nasa['pl_name_norm'] = df_nasa['pl_name'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)

    df_merged = df_nasa # Start with NASA data

    if not df_local.empty and 'pl_name_local' in df_local.columns:
        df_local['pl_name_norm'] = df_local['pl_name_local'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)
        print(f"Merging NASA data ({len(df_nasa)}) with Local data ({len(df_local)})...")
        # Select columns from local data to merge, avoid duplicates except the key
        cols_to_merge = df_local.columns.difference(df_nasa.columns).tolist() + ['pl_name_norm']
        # Remove potentially redundant columns explicitly if needed
        for col in ['pl_name_local']: # Add others if necessary
             if col in cols_to_merge: cols_to_merge.remove(col)
        df_merged = pd.merge(df_nasa, df_local[cols_to_merge], on='pl_name_norm', how='left')
    else:
        print("Proceeding with NASA API data only (no local data merged).")


    # Cleanup merge keys
    df_merged = df_merged.drop(columns=[col for col in ['pl_name_norm'] if col in df_merged.columns], errors='ignore')
    print(f"Merge completed. Resulting DataFrame has {len(df_merged)} rows.")

    # 3. TRANSFORM (SCRAPING - Planet Type)
    df_enriched: pd.DataFrame = add_planet_type(df_merged)

    # 4. LOAD
    try:
        save_normalized_to_db(df_enriched, df_stars_api)
        print("Pipeline completed successfully.")
    except Exception as e:
        print(f"Pipeline failed during database loading.")
        # Re-raise the exception so main.py knows it failed
        raise e

if __name__ == "__main__":
    print("This script is intended to be run as a module via 'main.py' in the project root.")