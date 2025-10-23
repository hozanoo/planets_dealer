# main.py
import sys
from src.pipeline import run_pipeline

if __name__ == "__main__":

    print("Starting ETL process via main.py...")

    try:
        # Set the desired limit here, e.g. None for all
        run_pipeline(limit=100)
        print("ETL process finished successfully.")

    except Exception as e:
        print(f"ETL process finished with ERROR.")
        sys.exit(1) # Exit with error code