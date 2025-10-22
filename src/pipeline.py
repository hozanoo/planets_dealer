import os
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import io
import psycopg2
from psycopg2.extras import execute_values # Für Bulk Insert
from psycopg2.errors import UndefinedTable
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any, Optional # Für Type Hints

# Lade Umgebungsvariablen
BASE_DIR: Path = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')


# ==============================================================
# 1. EXTRACT (API - SYSTEME & PLANETEN)
# ==============================================================
def fetch_exoplanets(limit: Optional[int] = 100) -> pd.DataFrame:
    """
    Lädt System- und Planetendaten aus PSCompPars.
    Wenn limit=None oder limit=0, werden alle Zeilen geholt.
    """
    api_url: str = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

    top_clause = f"TOP {limit}" if limit and limit > 0 else ""

    query: str = f"""
        SELECT {top_clause}
            hostname, pl_name, disc_year, sy_snum, sy_pnum,
            pl_orbper, pl_orbsmax, pl_rade, pl_masse,
            pl_orbeccen, pl_eqt, pl_insol
        FROM PSCompPars
        WHERE sy_snum > 1
    """
    query = re.sub(r'\s+', ' ', query).strip()

    params: Dict[str, Any] = {"query": query, "format": "csv"}
    print(f"Lade System- & Planetendaten (PSCompPars) von NASA API{' mit Limit ' + str(limit) if limit and limit > 0 else ' (alle Daten)'}...")
    try:
        timeout_seconds = 60 if limit and limit > 0 else 300 # 5 Min. für "alle"
        r: requests.Response = requests.get(api_url, params=params, timeout=timeout_seconds)
        r.raise_for_status()
        if not r.text.strip():
            raise ValueError("Leere Antwort vom NASA-Server (PSCompPars).")
        df: pd.DataFrame = pd.read_csv(io.StringIO(r.text))
        print(f"{len(df)} Zeilen von PSCompPars API geladen.")
        return df
    except Exception as e:
        print(f"Fehler beim Abrufen der NASA-Daten (PSCompPars): {e}")
        return pd.DataFrame()

# ==============================================================
# 2. EXTRACT (LOKALE CSV 'hwc.csv')
# ==============================================================
def load_local_hab_data(csv_path: Path) -> pd.DataFrame:
    """Lädt die 'hwc.csv' und extrahiert ergänzende Spalten."""
    print(f"Lade Habitabilitäts-Daten aus '{csv_path}'...")
    try:
        df: pd.DataFrame = pd.read_csv(csv_path)

        needed_cols: Dict[str, str] = {
            # Planet
            "P_NAME": "pl_name_local", "P_ESI": "esi", "P_HABITABLE": "habitable",
            # "P_HABZONE_OPT": "p_habzone_opt", # Entfernt (3NF)
            # "P_HABZONE_CON": "p_habzone_con", # Entfernt (3NF)
            "P_DETECTION": "discoverymethod",
            "P_DISCOVERY_FACILITY": "disc_facility",
            # System
            "S_CONSTELLATION_ENG": "constellation", "S_DISTANCE": "sy_dist_pc",
            # HZ Grenzen
            "S_HZ_CON_MIN": "hz_in_con",
            "S_HZ_CON_MAX": "hz_out_con",
            "S_HZ_OPT_MIN": "hz_in_opt",
            "S_HZ_OPT_MAX": "hz_out_opt"
        }

        all_cols_in_df: List[str] = df.columns.tolist()
        missing_cols: List[str] = [col for col in needed_cols.keys() if col not in all_cols_in_df]
        if missing_cols:
             raise KeyError(f"Folgende Spalten fehlen in '{csv_path.name}': {missing_cols}.")

        df_renamed: pd.DataFrame = df[needed_cols.keys()].rename(columns=needed_cols)

        # Boolean-Spalten nicht mehr nötig
        # if 'p_habzone_opt' in df_renamed.columns: ...
        # if 'p_habzone_con' in df_renamed.columns: ...

        print(f"{len(df_renamed)} Zeilen aus lokaler CSV geladen.")
        return df_renamed
    except FileNotFoundError:
        print(f"FEHLER: Die Datei '{csv_path}' wurde nicht gefunden.")
        return pd.DataFrame()
    except KeyError as e:
        print(f"FEHLER: {e}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"FEHLER beim Lesen der CSV '{csv_path}': {e}")
        return pd.DataFrame()

# ==============================================================
# 2.5 EXTRACT (API - STERNE via GROUP BY)
# ==============================================================
def fetch_stellar_hosts() -> pd.DataFrame:
    """Lädt aggregierte Sterndaten aus 'stellarhosts' via GROUP BY."""
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
    print("Lade aggregierte Sterndaten (stellarhosts per GROUP BY) von NASA API...")
    try:
        r: requests.Response = requests.get(api_url, params=params, timeout=120)
        r.raise_for_status()
        if not r.text.strip():
            raise ValueError("Leere Antwort vom NASA-Server (stellarhosts).")
        df: pd.DataFrame = pd.read_csv(io.StringIO(r.text))
        df_renamed: pd.DataFrame = df.rename(columns={'sy_name': 'system_key', 'hostname': 'star_name'})
        print(f"{len(df_renamed)} eindeutige Sterne von stellarhosts API geladen.")
        return df_renamed
    except Exception as e:
        print(f"Fehler beim Abrufen der stellarhosts-Daten: {e}")
        return pd.DataFrame()

# ==============================================================
# 3. TRANSFORM (SCRAPING - Planet Type)
# ==============================================================
def get_nasa_planet_type(planet_name: str) -> str:
    """Holt Beschreibung von NASA und extrahiert den Planetentyp."""
    planet_types: List[str] = ['Neptune-like', 'terrestrial', 'gas giant', 'super Earth']
    try:
        base_url: str = "https://science.nasa.gov/exoplanet-catalog/"
        formatted_name: str = planet_name.lower().replace(" ", "-")
        url: str = f"{base_url}{formatted_name}/"
        headers: Dict[str, str] = {"User-Agent": "Mozilla/5.0"}
        res: requests.Response = requests.get(url, timeout=10, headers=headers)
        if res.status_code == 404:
            return 'Unknown'
        soup: BeautifulSoup = BeautifulSoup(res.content, "html.parser")
        desc_element = soup.select_one("div.custom-field span")
        if desc_element:
            description_text: str = desc_element.get_text(strip=True).lower()
            for p_type in planet_types:
                if p_type.lower() in description_text:
                    original_type: str = next(t for t in planet_types if t.lower() == p_type.lower())
                    return original_type
    except Exception:
        pass
    return 'Unknown'

def add_planet_type(df: pd.DataFrame) -> pd.DataFrame:
    """Fügt die Spalte 'planet_type' durch Scraping hinzu."""
    if df.empty:
        print("Keine Daten zum Anreichern mit Planetentyp vorhanden.")
        return df
    print("Ermittle Planetentyp (NASA Scraping)... (Das kann dauern!)")
    df_copy: pd.DataFrame = df.copy()
    df_copy['planet_type'] = df_copy['pl_name'].apply(get_nasa_planet_type)
    print(f"Planetentypen hinzugefügt ({len(df_copy[df_copy['planet_type'] != 'Unknown'])} gefunden).")
    df_copy["visualization_url"] = df_copy["pl_name"].apply(
        lambda n: f"https://eyes.nasa.gov/apps/exo/#/planet/{n.replace(' ', '_')}"
    )
    return df_copy

# ==============================================================
# 4. LOAD (POSTGRESQL - 5 Tabellen)
# ==============================================================

class DatabaseError(Exception):
    pass

class ExoplanetDBPostgres:
    def __init__(self):
        try:
            self.connection: Optional[psycopg2.extensions.connection] = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'), host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT', 5432)
            )
            self.cursor: Optional[psycopg2.extensions.cursor] = self.connection.cursor()
            print(f"Erfolgreich mit DB '{os.environ.get('DB_NAME')}' auf Host '{os.environ.get('DB_HOST')}' verbunden.")
        except psycopg2.Error as e:
            self.connection = None; self.cursor = None
            raise DatabaseError(f"Fehler beim Verbinden mit der Datenbank: {e}")
        except Exception as e:
            self.connection = None; self.cursor = None
            raise ValueError(f"FEHLER: DB-Umgebungsvariablen nicht (vollständig) gesetzt? {e}")

    def _execute_values(self, query: str, data_tuples: List[Tuple], page_size: int = 100):
        """Hilfsfunktion für execute_values mit Fehlerbehandlung."""
        if not data_tuples: return
        if not self.cursor: raise DatabaseError("Keine Datenbankverbindung vorhanden.")
        try:
            execute_values(self.cursor, query, data_tuples, page_size=page_size)
        except psycopg2.Error as e:
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Fehler beim Ausführen von execute_values: {e}\nQuery Template: {query[:500]}...")
        except Exception as e:
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Unerwarteter Fehler in _execute_values: {e}")

    def _recreate_tables(self):
        """Erstellt das 5-Tabellen-Schema inkl. ENUM."""
        if not self.cursor or not self.connection: raise DatabaseError("Keine DB-Verbindung.")
        try:
            print("Setze Tabellen-Schema zurück (DROP/CREATE)...")
            self.cursor.execute("DROP TABLE IF EXISTS planeten CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS sterne CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS systeme CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS DetectionMethods CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS DiscoveryFacilities CASCADE;")
            self.cursor.execute("DROP TYPE IF EXISTS planet_type_enum CASCADE;")

            self.cursor.execute("CREATE TYPE planet_type_enum AS ENUM ('Neptune-like', 'terrestrial', 'gas giant', 'super Earth', 'Unknown');")
            self.cursor.execute("CREATE TABLE DiscoveryFacilities (facility_id SERIAL PRIMARY KEY, facility_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute("CREATE TABLE DetectionMethods (method_id SERIAL PRIMARY KEY, method_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute("""
                CREATE TABLE systeme (
                    system_key TEXT PRIMARY KEY, sy_snum INTEGER, sy_pnum INTEGER,
                    constellation TEXT, sy_dist_pc REAL, hz_in_con REAL, hz_out_con REAL,
                    hz_in_opt REAL, hz_out_opt REAL
                );""")
            self.cursor.execute("""
                CREATE TABLE sterne (
                    star_name TEXT PRIMARY KEY,
                    system_key TEXT REFERENCES systeme(system_key) ON DELETE SET NULL,
                    st_teff REAL, st_lum REAL, st_age REAL, st_met REAL
                );""")
            self.cursor.execute("""
                CREATE TABLE planeten (
                    pl_name TEXT PRIMARY KEY,
                    star_name TEXT REFERENCES sterne(star_name) ON DELETE SET NULL,
                    method_id INTEGER REFERENCES DetectionMethods(method_id) ON DELETE SET NULL,
                    facility_id INTEGER REFERENCES DiscoveryFacilities(facility_id) ON DELETE SET NULL,
                    planet_type planet_type_enum,
                    disc_year INTEGER, pl_orbper REAL, pl_orbsmax REAL, pl_rade REAL,
                    pl_masse REAL, pl_orbeccen REAL, pl_eqt REAL, pl_insol REAL, esi REAL,
                    habitable INTEGER,
                    -- p_habzone_con BOOLEAN, -- Entfernt (3NF)
                    -- p_habzone_opt BOOLEAN, -- Entfernt (3NF)
                    visualization_url TEXT
                );""")
            self.connection.commit()
            print("Tabellen (inkl. Lookups & ENUM) erfolgreich erstellt.")
        except psycopg2.Error as e:
            self.connection.rollback()
            raise DatabaseError(f"Fehler beim Erstellen der Tabellen: {e}")

    def _dataframe_to_tuples(self, df: pd.DataFrame) -> List[Tuple]:
        """Konvertiert DataFrame in Tuples (behandelt NaN/NA)."""
        df_clean: pd.DataFrame = df.astype(object).where(pd.notnull(df), None)
        df_clean = df_clean.replace({pd.NA: None})
        return [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

    def _insert_lookup_data(self, df_merged: pd.DataFrame) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Fügt Daten in Lookup-Tabellen ein und gibt Mapping-Dicts zurück."""
        if not self.cursor or not self.connection: raise DatabaseError("Keine DB-Verbindung.")
        method_map: Dict[str, int] = {}
        facility_map: Dict[str, int] = {}
        try:
            # DetectionMethods
            if 'discoverymethod' in df_merged.columns:
                unique_methods = df_merged['discoverymethod'].dropna().unique()
                method_tuples: List[Tuple] = [(name,) for name in unique_methods]
                if method_tuples:
                    insert_query = "INSERT INTO DetectionMethods (method_name) VALUES (%s) ON CONFLICT (method_name) DO NOTHING;"
                    self.cursor.executemany(insert_query, method_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT method_name, method_id FROM DetectionMethods")
                    method_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(method_map)} Methoden in DB eingefügt/gefunden.")

            # DiscoveryFacilities
            if 'disc_facility' in df_merged.columns:
                unique_facilities = df_merged['disc_facility'].dropna().unique()
                facility_tuples: List[Tuple] = [(name,) for name in unique_facilities]
                if facility_tuples:
                    insert_query = "INSERT INTO DiscoveryFacilities (facility_name) VALUES (%s) ON CONFLICT (facility_name) DO NOTHING;"
                    self.cursor.executemany(insert_query, facility_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT facility_name, facility_id FROM DiscoveryFacilities")
                    facility_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(facility_map)} Facilities in DB eingefügt/gefunden.")

            return method_map, facility_map
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Fehler beim Einfügen der Lookup-Daten: {e}")

    def insert_data(self, df_main_merged: pd.DataFrame, df_sterne_api: pd.DataFrame):
        """Teilt DataFrames auf und fügt sie in die 5 Tabellen ein."""
        if not self.cursor or not self.connection: raise DatabaseError("Keine DB-Verbindung.")
        if df_main_merged.empty or df_sterne_api.empty:
            print("Keine Daten zum Einfügen vorhanden (eine Quelle ist leer).")
            return
        try:
            self._recreate_tables()
            method_map, facility_map = self._insert_lookup_data(df_main_merged)

            # Systeme
            print("Bereite 'systeme'-Daten vor...")
            df_systeme: pd.DataFrame = df_main_merged.rename(columns={'hostname': 'system_key'})
            system_cols: List[str] = [
                'system_key', 'sy_snum', 'sy_pnum', 'constellation', 'sy_dist_pc',
                'hz_in_con', 'hz_out_con', 'hz_in_opt', 'hz_out_opt'
            ]
            valid_system_cols: List[str] = [col for col in system_cols if col in df_systeme.columns]
            df_systeme = df_systeme[valid_system_cols].drop_duplicates(subset=['system_key']).dropna(subset=['system_key'])
            systeme_tuples: List[Tuple] = self._dataframe_to_tuples(df_systeme)

            # Sterne
            print("Bereite 'sterne'-Daten vor...")
            star_cols: List[str] = ['star_name', 'system_key', 'st_teff', 'st_lum', 'st_age', 'st_met']
            valid_star_cols = [col for col in star_cols if col in df_sterne_api.columns] # Nur vorhandene Spalten nehmen
            df_sterne: pd.DataFrame = df_sterne_api[valid_star_cols].copy()
            df_sterne = df_sterne[df_sterne['system_key'].isin(df_systeme['system_key'])]
            df_sterne = df_sterne.drop_duplicates(subset=['star_name']).dropna(subset=['star_name'])
            sterne_tuples: List[Tuple] = self._dataframe_to_tuples(df_sterne) # Verwende DataFrame mit validen Spalten

            # Planeten
            print("Bereite 'planeten'-Daten vor...")
            df_planeten_prep: pd.DataFrame = df_main_merged.rename(columns={'hostname': 'star_name'})
            df_planeten_prep['method_id'] = df_planeten_prep['discoverymethod'].map(method_map) if 'discoverymethod' in df_planeten_prep.columns else None
            df_planeten_prep['facility_id'] = df_planeten_prep['disc_facility'].map(facility_map) if 'disc_facility' in df_planeten_prep.columns else None

            planet_cols: List[str] = [ # Reihenfolge wie in CREATE TABLE
                'pl_name', 'star_name', 'method_id', 'facility_id', 'planet_type',
                'disc_year', 'pl_orbper', 'pl_orbsmax', 'pl_rade', 'pl_masse',
                'pl_orbeccen', 'pl_eqt', 'pl_insol', 'esi', 'habitable',
                # 'p_habzone_con', 'p_habzone_opt', # Entfernt (3NF)
                'visualization_url'
            ]
            valid_planet_cols: List[str] = [col for col in planet_cols if col in df_planeten_prep.columns]
            df_planeten: pd.DataFrame = df_planeten_prep[valid_planet_cols]
            df_planeten = df_planeten.drop_duplicates(subset=['pl_name']).dropna(subset=['pl_name'])
            df_planeten = df_planeten[df_planeten['star_name'].isin(df_sterne['star_name'])]
            planeten_tuples: List[Tuple] = self._dataframe_to_tuples(df_planeten)

            # Insert
            print(f"Füge {len(systeme_tuples)} Systeme in DB ein...")
            cols_systeme_str: str = ", ".join(valid_system_cols)
            query_systeme: str = f"INSERT INTO systeme ({cols_systeme_str}) VALUES %s ON CONFLICT (system_key) DO NOTHING"
            self._execute_values(query_systeme, systeme_tuples)

            print(f"Füge {len(sterne_tuples)} Sterne in DB ein...")
            cols_sterne_str: str = ", ".join(valid_star_cols)
            query_sterne: str = f"INSERT INTO sterne ({cols_sterne_str}) VALUES %s ON CONFLICT (star_name) DO NOTHING"
            self._execute_values(query_sterne, sterne_tuples) # Verwende ursprüngliche sterne_tuples

            print(f"Füge {len(planeten_tuples)} Planeten in DB ein...")
            cols_planeten_str: str = ", ".join(valid_planet_cols)
            query_planeten: str = f"INSERT INTO planeten ({cols_planeten_str}) VALUES %s ON CONFLICT (pl_name) DO NOTHING"
            self._execute_values(query_planeten, planeten_tuples)

            self.connection.commit()
            print("Daten erfolgreich in alle fünf Tabellen geschrieben.")
        except (Exception, psycopg2.Error) as e:
            print(f"FEHLER beim Einfügen der Daten: {e}")
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Transaktion fehlgeschlagen: {e}")

    def close_connection(self):
        try:
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
            print("Datenbankverbindung geschlossen.")
        except psycopg2.Error as e:
            print(f"Fehler beim Schließen der DB-Verbindung: {e}")
        finally:
            self.cursor = None
            self.connection = None

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close_connection()

def save_normalized_to_db(df_main_merged: pd.DataFrame, df_sterne_api: pd.DataFrame):
    """Speichert die DataFrames in fünf normalisierten Tabellen (3NF)."""
    try:
        with ExoplanetDBPostgres() as db:
            db.insert_data(df_main_merged, df_sterne_api)
    except (DatabaseError, ValueError) as e:
        print(e)
        raise DatabaseError(f"Fehler in save_normalized_to_db: {e}")

# ==============================================================
# 5. ORCHESTRATION (MAIN)
# ==============================================================
def run_pipeline(limit: Optional[int] = 100): # Standard-Limit auf 100
    """Führt die gesamte ETL-Pipeline aus."""
    print(f"Starte Exoplaneten ETL-Pipeline (5 Tabellen) mit Limit={limit if limit else 'None'}...")

    # 1. EXTRACT
    df_nasa: pd.DataFrame = fetch_exoplanets(limit=limit)
    if df_nasa.empty: return

    local_csv_path: Path = BASE_DIR / 'data' / 'hwc.csv'
    df_local: pd.DataFrame = load_local_hab_data(local_csv_path)
    if df_local.empty: print("Warnung: Lokale CSV konnte nicht geladen werden oder ist leer.")

    df_sterne_api: pd.DataFrame = fetch_stellar_hosts()
    if df_sterne_api.empty: return

    # 2. TRANSFORM (MERGE - NASA API + Lokale CSV)
    df_nasa['pl_name_norm'] = df_nasa['pl_name'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)
    if not df_local.empty:
        df_local['pl_name_norm'] = df_local['pl_name_local'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)
        print(f"Führe Daten von NASA ({len(df_nasa)}) und Lokal ({len(df_local)}) zusammen...")
        cols_to_use = df_local.columns.difference(df_nasa.columns).tolist() + ['pl_name_norm']
        if 'pl_name_local' in cols_to_use: cols_to_use.remove('pl_name_local')
        df_merged: pd.DataFrame = pd.merge(df_nasa, df_local[cols_to_use], on='pl_name_norm', how='left')
    else:
        df_merged = df_nasa # Nur NASA-Daten

    # Aufräumen
    df_merged = df_merged.drop(columns=[col for col in ['pl_name_norm', 'pl_name_norm_x', 'pl_name_norm_y'] if col in df_merged.columns])
    print(f"Merge abgeschlossen. DataFrame hat {len(df_merged)} Zeilen.")

    # 3. TRANSFORM (SCRAPING - Planet Type)
    df_enriched: pd.DataFrame = add_planet_type(df_merged)

    # 4. LOAD
    try:
        save_normalized_to_db(df_enriched, df_sterne_api)
        print("Pipeline erfolgreich abgeschlossen.")
    except Exception as e:
        print(f"Pipeline mit Fehler bei DB-Speicherung gescheitert.")
        raise e

if __name__ == "__main__":
    print("Dieses Skript ist als Modul gedacht. Bitte starte es über 'main.py' im Hauptverzeichnis.")