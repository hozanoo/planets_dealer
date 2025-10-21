# main.py
import sys
from src.pipeline import run_pipeline

if __name__ == "__main__":

    print("Starte ETL-Prozess via main.py...")

    try:
        run_pipeline()
        # Diese Zeile wird nur bei Erfolg erreicht:
        print("ETL-Prozess erfolgreich beendet.")

    except Exception as e:
        print(f"ETL-Prozess mit FEHLER beendet.")
        sys.exit(1)