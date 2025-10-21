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
# 1. EXTRACT (API - SYSTEME & PLANETEN)
# ==============================================================
def fetch_exoplanets(limit=200):
    """
    Lädt System- und Planetendaten aus dem NASA Exoplanet Archive (PSCompPars).
    """
    api_url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    query = f"""
        SELECT TOP {limit}
            hostname, pl_name, disc_year, sy_snum, sy_pnum,
            pl_rade, pl_masse, pl_orbsmax, pl_orbeccen, pl_eqt, pl_insol
        FROM PSCompPars
        WHERE sy_snum>1
    """
    params = {"query": query, "format": "csv"}

    print("Lade System- & Planetendaten (PSCompPars) von NASA API...")
    try:
        r = requests.get(api_url, params=params, timeout=60)
        r.raise_for_status()

        if not r.text.strip():
            raise ValueError("Leere Antwort vom NASA-Server (PSCompPars).")

        df = pd.read_csv(io.StringIO(r.text))
        print(f"{len(df)} Zeilen von PSCompPars API geladen.")
        return df

    except Exception as e:
        print(f"Fehler beim Abrufen der NASA-Daten (PSCompPars): {e}")
        return pd.DataFrame()


# ==============================================================
# 2. EXTRACT (LOKALE CSV 'hwc.csv')
# ==============================================================
def load_local_hab_data(csv_path):
    """
    Lädt die 'hwc.csv' und extrahiert ergänzende Spalten (ESI, Sternbild).
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
# 2.5 EXTRACT (API - STERNE via GROUP BY)
# ==============================================================
def fetch_stellar_hosts():
    """
    Lädt Sterndaten aus 'stellarhosts'.
    Verwendet GROUP BY und AVG(), um für jeden Stern einen einzigen,
    eindeutigen (gemittelten) Wert für seine Attribute zu erhalten.
    """
    api_url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    query = """
            SELECT sy_name, \
                   hostname, \
                   AVG(st_teff) as st_teff, \
                   AVG(st_lum)  as st_lum, \
                   AVG(st_age)  as st_age, \
                   AVG(st_met)  as st_met
            FROM stellarhosts
            GROUP BY sy_name, hostname \
            """
    params = {"query": query, "format": "csv"}

    print("Lade aggregierte Sterndaten (stellarhosts per GROUP BY) von NASA API...")
    try:
        r = requests.get(api_url, params=params, timeout=120)  # Längerer Timeout
        r.raise_for_status()

        if not r.text.strip():
            raise ValueError("Leere Antwort vom NASA-Server (stellarhosts).")

        df = pd.read_csv(io.StringIO(r.text))

        # Spalten umbenennen, damit sie zu unserem DB-Schema passen
        df_renamed = df.rename(columns={
            'sy_name': 'system_key',  # Der System-Name (z.B. HD 23596)
            'hostname': 'star_name'  # Der Stern-Name (z.B. HD 23596 B)
        })

        print(f"{len(df_renamed)} eindeutige Sterne von stellarhosts API geladen.")
        return df_renamed

    except Exception as e:
        print(f"Fehler beim Abrufen der stellarhosts-Daten: {e}")
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
# 4. LOAD (POSTGRESQL - 3NF)
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
        Löscht alte Tabellen und erstellt das neue 3NF-Schema
        (Systeme -> Sterne -> Planeten).
        """
        try:
            print("Setze Tabellen-Schema zurück (DROP/CREATE)...")
            self.cursor.execute("DROP TABLE IF EXISTS planeten CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS sterne CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS systeme CASCADE;")

            # Tabelle 1: Systeme (höchste Ebene)
            self.cursor.execute("""
                                CREATE TABLE systeme
                                (
                                    system_key    TEXT PRIMARY KEY,
                                    sy_snum       INTEGER,
                                    sy_pnum       INTEGER,
                                    constellation TEXT,
                                    sy_dist_pc    REAL
                                );
                                """)

            # Tabelle 2: Sterne (verweist auf Systeme)
            self.cursor.execute("""
                                CREATE TABLE sterne
                                (
                                    star_name  TEXT PRIMARY KEY,
                                    system_key TEXT REFERENCES systeme (system_key) ON DELETE SET NULL,
                                    st_teff    REAL,
                                    st_lum     REAL,
                                    st_age     REAL,
                                    st_met     REAL
                                );
                                """)

            # Tabelle 3: Planeten (verweist auf Sterne)
            self.cursor.execute("""
                                CREATE TABLE planeten
                                (
                                    pl_name           TEXT PRIMARY KEY,
                                    star_name         TEXT REFERENCES sterne (star_name) ON DELETE SET NULL,
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
            print("Tabellen 'systeme', 'sterne' und 'planeten' erfolgreich erstellt.")
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

    def insert_data(self, df_main_merged, df_sterne_api):
        """
        Teilt die DataFrames auf und fügt sie in die 3NF-Tabellen ein.
        """
        if df_main_merged.empty or df_sterne_api.empty:
            print("Keine Daten zum Einfügen vorhanden (eine Quelle ist leer).")
            return

        try:
            self._recreate_tables()

            # 1. Tabelle: Systeme
            print("Bereite 'systeme'-Daten vor...")
            df_systeme = df_main_merged.rename(columns={'hostname': 'system_key'})
            system_cols = ['system_key', 'sy_snum', 'sy_pnum', 'constellation', 'sy_dist_pc']
            df_systeme = df_systeme[system_cols].drop_duplicates(subset=['system_key']).dropna(subset=['system_key'])
            systeme_tuples = self._dataframe_to_tuples(df_systeme)

            # 2. Tabelle: Sterne
            print("Bereite 'sterne'-Daten vor...")
            star_cols = ['star_name', 'system_key', 'st_teff', 'st_lum', 'st_age', 'st_met']
            df_sterne = df_sterne_api[star_cols].copy()
            df_sterne = df_sterne[df_sterne['system_key'].isin(df_systeme['system_key'])]
            # drop_duplicates ist hier jetzt sicher, da wir in der API schon aggregiert haben
            df_sterne = df_sterne.drop_duplicates(subset=['star_name']).dropna(subset=['star_name'])
            sterne_tuples = self._dataframe_to_tuples(df_sterne)

            # 3. Tabelle: Planeten
            print("Bereite 'planeten'-Daten vor...")
            df_planeten = df_main_merged.rename(columns={'hostname': 'star_name'})
            planet_cols = [
                'pl_name', 'star_name', 'disc_year', 'pl_rade', 'pl_masse',
                'pl_orbsmax', 'pl_orbeccen', 'pl_eqt', 'pl_insol', 'esi',
                'habitable', 'description_nasa', 'visualization_url'
            ]
            df_planeten = df_planeten[planet_cols].drop_duplicates(subset=['pl_name']).dropna(subset=['pl_name'])
            df_planeten = df_planeten[df_planeten['star_name'].isin(df_sterne['star_name'])]
            planeten_tuples = self._dataframe_to_tuples(df_planeten)

            # 4. Insert (Reihenfolge wichtig: Systeme -> Sterne -> Planeten)
            print(f"Füge {len(systeme_tuples)} Systeme in DB ein...")
            cols_systeme = ", ".join(system_cols)
            placeholders_systeme = ", ".join(["%s"] * len(system_cols))
            query_systeme = f"INSERT INTO systeme ({cols_systeme}) VALUES ({placeholders_systeme})"
            self.cursor.executemany(query_systeme, systeme_tuples)

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
            print("Daten erfolgreich in alle drei Tabellen geschrieben.")

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


def save_normalized_to_db(df_main_merged, df_sterne_api):
    """
    Speichert die DataFrames in drei normalisierten Tabellen (3NF).
    """
    try:
        with ExoplanetDBPostgres() as db:
            db.insert_data(df_main_merged, df_sterne_api)
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
    print("Starte Exoplaneten ETL-Pipeline (3NF)...")

    # 1. EXTRACT (API - Systeme & Planeten)
    df_nasa = fetch_exoplanets(limit=2000)
    if df_nasa.empty:
        print("Pipeline gestoppt: Keine Daten von NASA API (PSCompPars).")
        return

    # 2. EXTRACT (LOKAL - Ergänzungen)
    local_csv_path = BASE_DIR / 'data' / 'hwc.csv'
    df_local = load_local_hab_data(local_csv_path)
    if df_local.empty:
        print("Warnung: Lokale Habitabilitäts-Daten (hwc.csv) konnten nicht geladen werden.")

    # 2.5 EXTRACT (API - Sterne)
    df_sterne_api = fetch_stellar_hosts()
    if df_sterne_api.empty:
        print("Pipeline gestoppt: Keine Sterndaten (stellarhosts) von API geladen.")
        return

    # 3. TRANSFORM (MERGE - Systeme & Planeten mit lokalen Daten)
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
        save_normalized_to_db(df_enriched, df_sterne_api)
        print("Pipeline erfolgreich abgeschlossen.")
    except Exception as e:
        print(f"Pipeline mit Fehler bei DB-Speicherung gescheitert.")
        raise e


if __name__ == "__main__":
    print("Dieses Skript ist als Modul gedacht. Bitte starte es über 'main.py' im Hauptverzeichnis.")