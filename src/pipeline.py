import os
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import io
import psycopg2
from psycopg2.errors import UndefinedTable
from pathlib import Path
from dotenv import load_dotenv

# Lade Umgebungsvariablen aus der .env-Datei im Projekt-Root
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')


# ==============================================================
# 1. EXTRACT (API)
# ==============================================================
def fetch_exoplanets(limit=200):
    """
    Lädt Exoplanet-Daten aus dem NASA Exoplanet Archive (PSCompPars).
    """
    api_url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    query = f"""
        SELECT TOP {limit}
            hostname, pl_name, disc_year, sy_snum, sy_pnum,
            st_teff, st_lum, st_age, st_met,
            pl_rade, pl_masse, pl_orbsmax, pl_orbeccen, pl_eqt, pl_insol
        FROM PSCompPars
    """
    params = {"query": query, "format": "csv"}

    print("Lade Exoplanetendaten von NASA API...")
    try:
        r = requests.get(api_url, params=params, timeout=60)
        r.raise_for_status()

        if not r.text.strip():
            raise ValueError("Leere Antwort vom NASA-Server.")

        df = pd.read_csv(io.StringIO(r.text))
        print(f"{len(df)} Exoplaneten von NASA API geladen.")
        return df

    except Exception as e:
        print(f"Fehler beim Abrufen der NASA-Daten: {e}")
        return pd.DataFrame()


# ==============================================================
# 2. EXTRACT (LOKALE CSV 'hwc.csv')
# ==============================================================
def load_local_hab_data(csv_path):
    """
    Lädt die 'hwc.csv' und extrahiert die wichtigen Spalten.
    """
    print(f"Lade Habitabilitäts-Daten aus '{csv_path}'...")
    try:
        df = pd.read_csv(csv_path)

        needed_cols = {
            "P_NAME": "pl_name_local",
            "P_ESI": "esi",
            "P_HABITABLE": "habitable",
            "S_CONSTELLATION_ENG": "constellation",
            "S_DISTANCE": "sy_dist_pc"
        }

        missing_cols = [col for col in needed_cols.keys() if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Fehlende Spalten in hwc.csv: {missing_cols}")

        df_renamed = df[needed_cols.keys()].rename(columns=needed_cols)
        print(f"{len(df_renamed)} Planeten (mit ESI & Distanz) geladen.")
        return df_renamed

    except FileNotFoundError:
        print(f"FEHLER: Die Datei '{csv_path}' wurde nicht gefunden.")
        return pd.DataFrame()
    except KeyError as e:
        print(f"FEHLER: {e}.")
        return pd.DataFrame()


# ==============================================================
# 3. TRANSFORM (SCRAPING)
# ==============================================================
def get_nasa_description(planet_name):
    """
    Holt eine Kurzbeschreibung von der NASA-Website.
    """
    try:
        base_url = "https://science.nasa.gov/exoplanet-catalog/"
        formatted_name = planet_name.lower().replace(" ", "-")
        url = f"{base_url}{formatted_name}/"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, timeout=10, headers=headers)
        if res.status_code == 404: return None
        soup = BeautifulSoup(res.content, "html.parser")
        desc = soup.select_one("div.custom-field span")
        if desc:
            text = re.sub(r"\s+", " ", desc.get_text(strip=True))
            return text if len(text) > 20 else None
    except Exception:
        pass
    return None


def enrich_with_description(df):
    """
    Fügt die NASA-Beschreibung und Visualisierungs-URL hinzu.
    """
    if df.empty:
        print("Keine Daten zum Anreichern vorhanden.")
        return df
    print("Ergänze Planetenbeschreibungen (NASA)... (Das kann dauern!)")
    df["description_nasa"] = df["pl_name"].apply(get_nasa_description)
    df["visualization_url"] = df["pl_name"].apply(
        lambda n: f"https://eyes.nasa.gov/apps/exo/#/planet/{n.replace(' ', '_')}"
    )
    print(f"Beschreibungen hinzugefügt ({df['description_nasa'].notna().sum()} gefunden).")
    return df


# ==============================================================
# 4. LOAD (POSTGRESQL - ALS KLASSE)
# ==============================================================

class DatabaseError(Exception):
    pass


class ExoplanetDBPostgres:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT', 5432)
            )
            self.cursor = self.connection.cursor()
            print(f"Erfolgreich mit DB '{os.environ.get('DB_NAME')}' auf Host '{os.environ.get('DB_HOST')}' verbunden.")
        except psycopg2.Error as e:
            raise DatabaseError(f"Fehler beim Verbinden mit der Datenbank: {e}")
        except Exception as e:
            raise ValueError(f"FEHLER: DB-Umgebungsvariablen nicht (vollständig) gesetzt? {e}")

    def _recreate_tables(self):
        """
        Löscht alte Tabellen (falls vorhanden) und erstellt das 3NF-Schema neu.
        """
        try:
            print("Setze Tabellen-Schema zurück (DROP/CREATE)...")
            self.cursor.execute("DROP TABLE IF EXISTS planeten CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS sterne CASCADE;")

            self.cursor.execute("""
                                CREATE TABLE sterne
                                (
                                    hostname      TEXT PRIMARY KEY,
                                    sy_snum       INTEGER,
                                    sy_pnum       INTEGER,
                                    st_teff       REAL,
                                    st_lum        REAL,
                                    st_age        REAL,
                                    st_met        REAL,
                                    constellation TEXT,
                                    sy_dist_pc    REAL
                                );
                                """)

            self.cursor.execute("""
                                CREATE TABLE planeten
                                (
                                    pl_name           TEXT PRIMARY KEY,
                                    hostname          TEXT REFERENCES sterne (hostname) ON DELETE SET NULL,
                                    disc_year         INTEGER,
                                    pl_rade           REAL,
                                    pl_masse          REAL,
                                    pl_orbsmax        REAL,
                                    pl_orbeccen       REAL,
                                    pl_eqt            REAL,
                                    pl_insol          REAL,
                                    esi               REAL,
                                    habitable         INTEGER,
                                    description_nasa  TEXT,
                                    visualization_url TEXT
                                );
                                """)
            self.connection.commit()
            print("Tabellen 'sterne' und 'planeten' erfolgreich erstellt.")
        except psycopg2.Error as e:
            self.connection.rollback()
            raise DatabaseError(f"Fehler beim Erstellen der Tabellen: {e}")

    def _dataframe_to_tuples(self, df):
        """
        Konvertiert einen Pandas DataFrame in eine Liste von Tuples.
        Wandelt Pandas 'NaN' in 'None' um (für SQL NULL).
        """
        df_clean = df.astype(object).where(pd.notnull(df), None)
        return [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

    def insert_data(self, df_merged):
        """
        Nimmt den gesamten gemerged DataFrame, teilt ihn auf
        und fügt ihn in die normalisierten Tabellen 'sterne' und 'planeten' ein.
        """
        if df_merged.empty:
            print("Keine Daten zum Einfügen vorhanden.")
            return

        try:
            self._recreate_tables()

            print("Bereite 'sterne'-Daten vor...")
            star_cols = [
                'hostname', 'sy_snum', 'sy_pnum', 'st_teff', 'st_lum',
                'st_age', 'st_met', 'constellation', 'sy_dist_pc'
            ]
            df_sterne = df_merged[star_cols].drop_duplicates(subset=['hostname']).dropna(subset=['hostname'])
            sterne_tuples = self._dataframe_to_tuples(df_sterne)

            print("Bereite 'planeten'-Daten vor...")
            planet_cols = [
                'pl_name', 'hostname', 'disc_year', 'pl_rade', 'pl_masse',
                'pl_orbsmax', 'pl_orbeccen', 'pl_eqt', 'pl_insol', 'esi',
                'habitable', 'description_nasa', 'visualization_url'
            ]
            df_planeten = df_merged[planet_cols].drop_duplicates(subset=['pl_name']).dropna(subset=['pl_name'])

            df_planeten = df_planeten[df_planeten['hostname'].isin(df_sterne['hostname'])]
            planeten_tuples = self._dataframe_to_tuples(df_planeten)

            print(f"Füge {len(sterne_tuples)} Sterne in DB ein...")
            cols_sterne = ", ".join(star_cols)
            placeholders_sterne = ", ".join(["%s"] * len(star_cols))
            query_sterne = f"INSERT INTO sterne ({cols_sterne}) VALUES ({placeholders_sterne})"
            self.cursor.executemany(query_sterne, sterne_tuples)

            print(f"Füge {len(planeten_tuples)} Planeten in DB ein...")
            cols_planeten = ", ".join(planet_cols)
            placeholders_planeten = ", ".join(["%s"] * len(planet_cols))
            query_planeten = f"INSERT INTO planeten ({cols_planeten}) VALUES ({placeholders_planeten})"
            self.cursor.executemany(query_planeten, planeten_tuples)

            self.connection.commit()
            print("Daten erfolgreich in beide Tabellen geschrieben.")

        except (Exception, psycopg2.Error) as e:
            print(f"FEHLER beim Einfügen der Daten: {e}")
            self.connection.rollback()
            raise DatabaseError(f"Transaktion fehlgeschlagen: {e}")

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Datenbankverbindung geschlossen.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()


def save_normalized_to_db(df_merged):
    """
    Speichert den DataFrame in zwei normalisierten Tabellen (3NF)
    unter Verwendung der ExoplanetDBPostgres-Klasse.
    """
    try:
        with ExoplanetDBPostgres() as db:
            db.insert_data(df_merged)
    except (DatabaseError, ValueError) as e:
        print(e)
        raise DatabaseError(f"Fehler in save_normalized_to_db: {e}")


# ==============================================================
# 5. ORCHESTRATION (MAIN)
# ==============================================================
def run_pipeline():
    """
    Führt die gesamte ETL-Pipeline aus.
    """
    print("Starte Exoplaneten ETL-Pipeline (mit DB-Klasse)...")

    # 1. EXTRACT (API)
    df_nasa = fetch_exoplanets(limit=50)
    if df_nasa.empty:
        print("Pipeline gestoppt: Keine Daten von NASA API.")
        return

    # 2. EXTRACT (LOKAL)
    local_csv_path = BASE_DIR / 'data' / 'hwc.csv'
    df_local = load_local_hab_data(local_csv_path)
    if df_local.empty:
        print("Warnung: Lokale Habitabilitäts-Daten konnten nicht geladen werden.")

    # 3. TRANSFORM (MERGE)
    df_nasa['pl_name_norm'] = df_nasa['pl_name'].str.lower().str.replace(r'[\s-]+', '', regex=True)
    df_local['pl_name_norm'] = df_local['pl_name_local'].str.lower().str.replace(r'[\s-]+', '', regex=True)

    print(f"Führe Daten von NASA ({len(df_nasa)}) und Lokal ({len(df_local)}) zusammen...")
    df_merged = pd.merge(
        df_nasa,
        df_local,
        on='pl_name_norm',
        how='left'
    )

    if 'pl_name_local' in df_merged.columns:
        df_merged = df_merged.drop(columns=['pl_name_local'])
    df_merged = df_merged.drop(
        columns=[col for col in ['pl_name_norm', 'pl_name_norm_x', 'pl_name_norm_y'] if col in df_merged.columns])
    print(f"Merge abgeschlossen. DataFrame hat {len(df_merged)} Zeilen.")

    # 4. TRANSFORM (SCRAPING)
    df_enriched = enrich_with_description(df_merged)

    # 5. LOAD (Normalisiert via Klasse)
    try:
        save_normalized_to_db(df_enriched)
        print("Pipeline erfolgreich abgeschlossen.")
    except Exception as e:
        print(f"Pipeline mit Fehler bei DB-Speicherung gescheitert.")
        raise e


if __name__ == "__main__":
    print("Dieses Skript ist als Modul gedacht. Bitte starte es über 'main.py' im Hauptverzeichnis.")