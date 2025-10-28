# src/local_loader.py
import pandas as pd
from pathlib import Path
from typing import Dict, List

def load_local_data(csv_path: Path) -> pd.DataFrame:
    """
    Loads supplementary data from the local 'hwc.csv' file.

    This function reads the specified CSV file, selects a predefined
    set of columns, renames them to the internal standard names,
    and returns them as a DataFrame.

    :param csv_path: The file path to the 'hwc.csv' file.
    :type csv_path: Path
    :raises KeyError: If required columns are missing from the CSV file.
    :raises FileNotFoundError: If the CSV file is not found at the specified path.
    :return: A DataFrame containing the supplementary data.
    :rtype: pd.DataFrame
    """
    print(f"Lade ergänzende Daten aus '{csv_path}'...")
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
             raise KeyError(f"Fehlende Spalten in '{csv_path.name}': {missing_cols}.")

        df_renamed: pd.DataFrame = df[original_needed_cols].rename(columns=needed_cols_mapping)
        print(f"{len(df_renamed)} Zeilen aus lokaler CSV geladen.")
        return df_renamed
    except FileNotFoundError:
        print(f"FEHLER: Datei '{csv_path}' nicht gefunden.")
        return pd.DataFrame()
    except (KeyError, Exception) as e:
        print(f"FEHLER beim Lesen der CSV '{csv_path}': {e}")
        return pd.DataFrame()