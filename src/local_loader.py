import pandas as pd
from pathlib import Path
from typing import Dict, List

def load_local_data(csv_path: Path) -> pd.DataFrame:
    """Loads supplementary data from the local 'hwc.csv'."""
    print(f"Loading supplementary data from '{csv_path}'...")
    try:
        df: pd.DataFrame = pd.read_csv(csv_path)
        needed_cols_mapping: Dict[str, str] = {
            "P_NAME": "pl_name_local", "P_ESI": "esi",
            "P_DETECTION": "detection_method_name",
            "P_DISCOVERY_FACILITY": "facility_name",
            "S_CONSTELLATION_ENG": "constellation_en", "S_DISTANCE": "distance_pc",
            "S_HZ_CON_MIN": "hz_conservative_inner_au", "S_HZ_CON_MAX": "hz_conservative_outer_au",
            "S_HZ_OPT_MIN": "hz_optimistic_inner_au", "S_HZ_OPT_MAX": "hz_optimistic_outer_au"
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