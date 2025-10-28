# src/pipeline.py
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, List

# Relative imports for modules within the same package
from .api_logger import fetch_exoplanets, fetch_stellar_hosts
from .local_loader import load_local_data
from .web_logger import add_planet_type
from .save_data import save_normalized_to_db, DatabaseError

# Load environment variables from the project root
BASE_DIR: Path = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')


def run_pipeline(limit: Optional[int] = 100):
    """
    Executes the entire ETL pipeline orchestration.

    This function coordinates the Extract, Transform, and Load steps:
    1.  Extracts data from NASA APIs (planets, stars) and local CSV.
    2.  Merges API data with local supplementary data.
    3.  Transforms data by enriching it with web-scraped planet types.
    4.  Loads the final, prepared DataFrames into the PostgreSQL database.

    :param limit: The maximum number of rows to fetch from the planet API.
                  If 0 or None, fetches all available rows.
    :type limit: Optional[int]
    """
    print(f"Starte Exoplaneten ETL-Pipeline (Modular, 5 Tabellen) mit Limit={limit if limit else 'None'}...")

    # ==============================================================
    # 1. EXTRACT
    # ==============================================================
    df_nasa: pd.DataFrame = fetch_exoplanets(limit=limit)
    if df_nasa.empty:
        print("Pipeline gestoppt: Keine Primärdaten von NASA API (PSCompPars) erhalten.")
        return

    local_csv_path: Path = BASE_DIR / 'data' / 'hwc.csv'
    df_local: pd.DataFrame = load_local_data(local_csv_path)
    if df_local.empty:
        print("Warnung: Lokale Zusatzdaten (hwc.csv) konnten nicht geladen werden oder sind leer. Fahre ohne sie fort.")

    df_stars_api: pd.DataFrame = fetch_stellar_hosts()
    if df_stars_api.empty:
        print("Pipeline gestoppt: Keine Sterndaten (stellarhosts) von API erhalten.")
        return

    # ==============================================================
    # 2. TRANSFORM (MERGE)
    # ==============================================================
    if 'pl_name' not in df_nasa.columns:
        print("FEHLER: 'pl_name'-Spalte fehlt in NASA API-Daten. Merge nicht möglich.")
        return

    # Erstelle normalisierten Merge-Schlüssel für API-Daten
    df_nasa['pl_name_norm'] = df_nasa['pl_name'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)

    df_merged = df_nasa  # Beginne mit den NASA-Daten

    # Führe Merge nur durch, wenn lokale Daten vorhanden UND gültig sind
    if not df_local.empty and 'pl_name_local' in df_local.columns:
        df_local['pl_name_norm'] = df_local['pl_name_local'].astype(str).str.lower().str.replace(r'[\s-]+', '',
                                                                                                 regex=True)
        print(f"Merge NASA-Daten ({len(df_nasa)}) mit lokalen Daten ({len(df_local)})...")

        # Wähle nur Spalten aus df_local aus, die nicht schon in df_nasa sind (außer dem Merge-Schlüssel)
        cols_to_merge = df_local.columns.difference(df_nasa.columns).tolist() + ['pl_name_norm']
        if 'pl_name_local' in cols_to_merge:
            cols_to_merge.remove('pl_name_local')  # Redundante Namensspalte entfernen

        df_merged = pd.merge(df_nasa, df_local[cols_to_merge], on='pl_name_norm', how='left')
    else:
        print("Fahre nur mit NASA API-Daten fort (keine lokalen Daten gemerged).")

    # Räume Merge-Schlüssel auf
    df_merged = df_merged.drop(columns=[col for col in ['pl_name_norm'] if col in df_merged.columns], errors='ignore')
    print(f"Merge abgeschlossen. Resultierender DataFrame hat {len(df_merged)} Zeilen.")

    # ==============================================================
    # 3. TRANSFORM (SCRAPING)
    # ==============================================================
    df_enriched: pd.DataFrame = add_planet_type(df_merged)

    # ==============================================================
    # 4. LOAD
    # ==============================================================
    try:
        save_normalized_to_db(df_enriched, df_stars_api)
        print("Pipeline erfolgreich abgeschlossen.")
    except Exception as e:
        print(f"Pipeline mit Fehler bei DB-Speicherung gescheitert.")
        raise e  # Fehler weitergeben, damit main.py ihn fängt