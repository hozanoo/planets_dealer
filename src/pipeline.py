import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, List

# Der Punkt am Anfang (.) bedeutet "aus dem gleichen Verzeichnis (src)"
from .api_logger import fetch_exoplanets, fetch_stellar_hosts
from .local_loader import load_local_data
from .web_logger import add_planet_type
from .save_data import save_normalized_to_db, DatabaseError

# Lade Umgebungsvariablen
BASE_DIR: Path = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')

def run_pipeline(limit: Optional[int] = 100):
    """Executes the entire ETL pipeline."""
    print(f"Starting Exoplanet ETL Pipeline (Modular, 5 tables) with limit={limit if limit else 'None'}...")

    # 1. EXTRACT
    df_nasa: pd.DataFrame = fetch_exoplanets(limit=limit)
    if df_nasa.empty:
        print("Pipeline stopped: No primary data fetched from NASA API.")
        return

    local_csv_path: Path = BASE_DIR / 'data' / 'hwc.csv'
    df_local: pd.DataFrame = load_local_data(local_csv_path)
    if df_local.empty:
        print("Warning: Local supplementary data could not be loaded or is empty. Proceeding without it.")

    df_stars_api: pd.DataFrame = fetch_stellar_hosts()
    if df_stars_api.empty:
        print("Pipeline stopped: No star data fetched from API.")
        return

    # 2. TRANSFORM (MERGE - API + Local CSV)
    if 'pl_name' not in df_nasa.columns:
        print("ERROR: 'pl_name' column missing from NASA API data. Cannot merge.")
        return
    df_nasa['pl_name_norm'] = df_nasa['pl_name'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)

    df_merged = df_nasa # Start with NASA data

    if not df_local.empty and 'pl_name_local' in df_local.columns:
        df_local['pl_name_norm'] = df_local['pl_name_local'].astype(str).str.lower().str.replace(r'[\s-]+', '', regex=True)
        print(f"Merging NASA data ({len(df_nasa)}) with Local data ({len(df_local)})...")
        cols_to_merge = df_local.columns.difference(df_nasa.columns).tolist() + ['pl_name_norm']
        for col in ['pl_name_local']:
             if col in cols_to_merge: cols_to_merge.remove(col)
        df_merged = pd.merge(df_nasa, df_local[cols_to_merge], on='pl_name_norm', how='left')
    else:
        print("Proceeding with NASA API data only (no local data merged).")

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
        raise e