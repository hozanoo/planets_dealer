# tests/test_save_data.py
import unittest
import psycopg2
import pandas as pd
import numpy as np  # Wird für np.nan benötigt
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any, Optional

# Lade Umgebungsvariablen (wichtig für DB-Zugangsdaten)
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')

# Importiere die zu testende Klasse und Exception
from src.save_data import ExoplanetDBPostgres, DatabaseError

# --- Test Konfiguration ---
TEST_DB_NAME = 'planets_test_db'
ADMIN_DB_NAME = os.environ.get('DB_NAME', 'postgres')
ADMIN_DB_USER = os.environ.get('DB_USER', 'postgres')
ADMIN_DB_PASSWORD = os.environ.get('DB_PASSWORD')
ADMIN_DB_HOST = os.environ.get('DB_HOST', 'localhost')
ADMIN_DB_PORT = os.environ.get('DB_PORT', 5432)


# --- Ende Test Konfiguration ---


class TestExoplanetDBIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Runs ONCE before all tests: Creates the test DB."""
        print("\n--- Initialisiere Testumgebung ---")
        try:
            cls.admin_conn = psycopg2.connect(
                dbname=ADMIN_DB_NAME, user=ADMIN_DB_USER,
                password=ADMIN_DB_PASSWORD, host=ADMIN_DB_HOST, port=ADMIN_DB_PORT
            )
            cls.admin_conn.autocommit = True
            cls.admin_cursor = cls.admin_conn.cursor()
            print(f"Lösche alte Test-DB '{TEST_DB_NAME}' (falls vorhanden)...")
            cls.admin_cursor.execute(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}";')
            print(f"Erstelle neue Test-DB '{TEST_DB_NAME}'...")
            cls.admin_cursor.execute(f'CREATE DATABASE "{TEST_DB_NAME}";')
            print("Test-DB erstellt.")
        except Exception as e:
            if hasattr(cls, 'admin_cursor') and cls.admin_cursor: cls.admin_cursor.close()
            if hasattr(cls, 'admin_conn') and cls.admin_conn: cls.admin_conn.close()
            raise ConnectionError(f"Konnte Test-DB nicht initialisieren: {e}")
        finally:
            if hasattr(cls, 'admin_cursor') and cls.admin_cursor: cls.admin_cursor.close()
            if hasattr(cls, 'admin_conn') and cls.admin_conn: cls.admin_conn.close()

    @classmethod
    def tearDownClass(cls):
        """Runs ONCE after all tests: Drops the test DB."""
        print("\n--- Räume Testumgebung auf ---")
        try:
            conn = psycopg2.connect(
                dbname=ADMIN_DB_NAME, user=ADMIN_DB_USER,
                password=ADMIN_DB_PASSWORD, host=ADMIN_DB_HOST, port=ADMIN_DB_PORT
            )
            conn.autocommit = True
            cursor = conn.cursor()
            print(f"Lösche Test-DB '{TEST_DB_NAME}'...")
            cursor.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{TEST_DB_NAME}';
            """)
            cursor.execute(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}";')
            print("Test-DB gelöscht.")
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"FEHLER beim Aufräumen der Test-DB: {e}")

    def setUp(self):
        """Runs BEFORE EACH test: Connects to the test DB."""
        os.environ['DB_NAME_ORIG'] = os.environ.get('DB_NAME', '')
        os.environ['DB_NAME'] = TEST_DB_NAME
        try:
            self.db = ExoplanetDBPostgres()
            self.db._recreate_tables()
            self.cursor = self.db.connection.cursor()
        except DatabaseError as e:
            self.fail(f"Fehler beim Verbinden mit der Test-DB in setUp: {e}")

    def tearDown(self):
        """Runs AFTER EACH test: Closes the connection."""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'db') and self.db:
            self.db.close_connection()
        os.environ['DB_NAME'] = os.environ.get('DB_NAME_ORIG', '')
        if 'DB_NAME_ORIG' in os.environ: del os.environ['DB_NAME_ORIG']

    def _create_dummy_data(self):
        """Creates consistent dummy DataFrames for testing."""
        df_main = pd.DataFrame({
            'pl_name': ['TestPlanet A', 'TestPlanet B', 'TestPlanet C'],
            'star_name_api': ['TestStar Alpha', 'TestStar Beta', 'TestStar Alpha'],
            'detection_method_name': ['Transit', 'Radial Velocity', 'Transit'],
            'facility_name': ['TestScope 1', 'TestScope 2', 'TestScope 1'],
            'planet_type': ['terrestrial', 'gas giant', 'Unknown'],
            'discovery_year': [2020, 2021, 2022],
            'orbital_period_days': [10.1, 300.5, 5.2],
            'orbit_semi_major_axis_au': [0.1, 1.5, 0.05],
            'planet_radius_earth_radii': [1.0, 11.0, 0.9],
            'planet_mass_earth_masses': [1.0, 300.0, 0.8],
            'esi': [0.9, 0.1, 0.8],
            'constellation_en': ['Test Const 1', 'Test Const 2', 'Test Const 1'],
            'distance_pc': [10.5, 50.2, 10.5],
            'hz_conservative_inner_au': [0.8, 0.9, 0.8],
            'hz_conservative_outer_au': [1.5, 1.8, 1.5],
            'hz_optimistic_inner_au': [0.7, 0.75, 0.7],
            'hz_optimistic_outer_au': [2.0, 2.2, 2.0]
        })

        df_stars = pd.DataFrame({
            'star_name': ['TestStar Alpha', 'TestStar Beta', 'TestStar Gamma'],
            'system_key': ['TestStar Alpha', 'TestStar Beta', 'TestStar Alpha'],
            'st_teff': [5800, 4500, 5900],
            'st_lum': [0.0, -0.5, 0.1],
            'st_age': [4.5, 8.0, 4.0],
            'st_met': [0.0, 0.1, -0.1]
        })
        return df_main, df_stars

    def test_01_insert_and_verify_data(self):
        """Tests data insertion and verifies the result."""
        df_main, df_stars = self._create_dummy_data()
        self.db.insert_data(df_main, df_stars)

        self.cursor.execute("SELECT COUNT(*) FROM detection_methods")
        self.assertEqual(self.cursor.fetchone()[0], 2, "Sollte 2 Methoden eingefügt haben")
        self.cursor.execute("SELECT COUNT(*) FROM discovery_facilities")
        self.assertEqual(self.cursor.fetchone()[0], 2, "Sollte 2 Facilities eingefügt haben")
        self.cursor.execute("SELECT COUNT(*) FROM systems")
        self.assertEqual(self.cursor.fetchone()[0], 2, "Sollte 2 Systeme eingefügt haben")
        self.cursor.execute("SELECT COUNT(*) FROM stars")
        self.assertEqual(self.cursor.fetchone()[0], 3, "Sollte 3 Sterne eingefügt haben")
        self.cursor.execute("SELECT COUNT(*) FROM planets")
        self.assertEqual(self.cursor.fetchone()[0], 3, "Sollte 3 Planeten eingefügt haben")

        self.cursor.execute(
            "SELECT planet_type, method_id, facility_id, visualization_url FROM planets WHERE pl_name = 'TestPlanet A'")
        result_A = self.cursor.fetchone()
        self.assertIsNotNone(result_A, "TestPlanet A sollte gefunden werden")
        self.assertEqual(result_A[0], 'terrestrial', "Planetentyp für A sollte terrestrial sein")
        self.assertIsNotNone(result_A[1], "method_id für A sollte nicht NULL sein")
        self.assertIsNotNone(result_A[2], "facility_id für A sollte nicht NULL sein")
        self.assertTrue(result_A[3].endswith("TestPlanet_A"), "Visualization URL sollte korrekt generiert sein")

        self.cursor.execute("SELECT planet_type FROM planets WHERE pl_name = 'TestPlanet C'")
        result_C = self.cursor.fetchone()
        self.assertIsNotNone(result_C, "TestPlanet C sollte gefunden werden")
        self.assertEqual(result_C[0], 'Unknown', "Planetentyp für C sollte Unknown sein")

    def test_02_foreign_key_constraints(self):
        """Tests if 'ON DELETE SET NULL' foreign key relationships work."""
        df_main, df_stars = self._create_dummy_data()
        self.db.insert_data(df_main, df_stars)

        try:
            self.cursor.execute("DELETE FROM stars WHERE star_name = 'TestStar Alpha'")
            self.db.connection.commit()
            self.cursor.execute("SELECT star_name FROM planets WHERE pl_name = 'TestPlanet A'")
            self.assertIsNone(self.cursor.fetchone()[0], "star_name in planets sollte nach Stern-Löschung NULL sein")
        except Exception as e:
            self.db.connection.rollback()
            self.fail(f"Löschen des Sterns sollte wegen ON DELETE SET NULL möglich sein: {e}")

        try:
            self.cursor.execute("DELETE FROM systems WHERE system_key = 'TestStar Beta'")
            self.db.connection.commit()
            self.cursor.execute("SELECT system_key FROM stars WHERE star_name = 'TestStar Beta'")
            self.assertIsNone(self.cursor.fetchone()[0], "system_key in stars sollte nach System-Löschung NULL sein")
        except Exception as e:
            self.db.connection.rollback()
            self.fail(f"Löschen des Systems sollte wegen ON DELETE SET NULL möglich sein: {e}")

    def test_03_idempotency_check(self):
        """Tests if multiple inserts (due to ON CONFLICT) cause no errors or duplicates."""
        df_main, df_stars = self._create_dummy_data()

        self.db.insert_data(df_main, df_stars)
        self.cursor.execute("SELECT COUNT(*) FROM planets")
        count_before = self.cursor.fetchone()[0]
        self.assertEqual(count_before, 3)

        try:
            self.db.insert_data(df_main, df_stars)
        except Exception as e:
            self.fail(f"Zweiter Insert-Lauf ist fehlgeschlagen, sollte aber ignoriert werden (ON CONFLICT): {e}")

        self.cursor.execute("SELECT COUNT(*) FROM planets")
        count_after = self.cursor.fetchone()[0]
        self.assertEqual(count_after, count_before, "Anzahl der Planeten sollte nach zweitem Lauf identisch sein")

    def test_04_empty_dataframes_do_not_crash(self):
        """Tests if the script can handle empty DataFrames."""
        df_main_empty = pd.DataFrame(columns=['pl_name', 'star_name_api'])
        df_stars_empty = pd.DataFrame(columns=['star_name', 'system_key'])
        df_main_full, df_stars_full = self._create_dummy_data()

        try:
            self.db.insert_data(df_main_empty, df_stars_empty)
            self.cursor.execute("SELECT COUNT(*) FROM planets")
            self.assertEqual(self.cursor.fetchone()[0], 0, "DB sollte leer sein, wenn beide DFs leer sind")

            self.db._recreate_tables()
            self.db.insert_data(df_main_full, df_stars_empty)
            self.cursor.execute("SELECT COUNT(*) FROM planets")
            self.assertEqual(self.cursor.fetchone()[0], 0, "Keine Planeten, wenn Sterne-DF leer ist")

            self.db._recreate_tables()
            self.db.insert_data(df_main_empty, df_stars_full)
            self.cursor.execute("SELECT COUNT(*) FROM planets")
            self.assertEqual(self.cursor.fetchone()[0], 0, "Keine Planeten, wenn Haupt-DF leer ist")

        except Exception as e:
            self.fail(f"insert_data ist mit leeren DataFrames abgestürzt: {e}")

    def test_05_insert_with_null_values(self):
        """Tests if pd.NA and None are correctly handled as SQL NULL."""
        df_main = pd.DataFrame({
            'pl_name': ['TestPlanet NULL'], 'star_name_api': ['TestStar NULL'],
            'detection_method_name': ['Transit'], 'facility_name': ['TestScope 1'],
            'planet_type': ['Unknown'], 'esi': [pd.NA]
        })
        df_stars = pd.DataFrame({
            'star_name': ['TestStar NULL'], 'system_key': ['TestStar NULL'],
            'st_age': [None]
        })

        self.db.insert_data(df_main, df_stars)

        self.cursor.execute("SELECT esi FROM planets WHERE pl_name = 'TestPlanet NULL'")
        self.assertIsNone(self.cursor.fetchone()[0], "pd.NA bei 'esi' sollte als SQL NULL gespeichert werden")

        self.cursor.execute("SELECT st_age FROM stars WHERE star_name = 'TestStar NULL'")
        self.assertIsNone(self.cursor.fetchone()[0], "None bei 'st_age' sollte als SQL NULL gespeichert werden")

    def test_06_filter_orphaned_data(self):
        """Tests if orphaned data (planets/stars without valid FK) is filtered out."""
        df_main, df_stars = self._create_dummy_data()

        df_stars_orphan = pd.DataFrame([
            {'star_name': 'Orphan Star', 'system_key': 'System ZZZ', 'st_teff': 5000}
        ])
        df_stars_combined = pd.concat([df_stars, df_stars_orphan], ignore_index=True)

        df_planets_orphan = pd.DataFrame([
            {'pl_name': 'Orphan Planet', 'star_name_api': 'Orphan Star', 'planet_type': 'terrestrial'}
        ])
        df_main_combined = pd.concat([df_main, df_planets_orphan], ignore_index=True)

        self.db.insert_data(df_main_combined, df_stars_combined)

        self.cursor.execute("SELECT * FROM stars WHERE star_name = 'Orphan Star'")
        self.assertIsNone(self.cursor.fetchone(), "Verwaister Stern 'Orphan Star' sollte nicht eingefügt werden")

        self.cursor.execute("SELECT * FROM planets WHERE pl_name = 'Orphan Planet'")
        self.assertIsNone(self.cursor.fetchone(), "Verwaister Planet 'Orphan Planet' sollte nicht eingefügt werden")

    def test_07_invalid_planet_type_enum(self):
        """Tests if an invalid ENUM value raises an error."""
        df_main, df_stars = self._create_dummy_data()

        df_main.loc[df_main['pl_name'] == 'TestPlanet A', 'planet_type'] = 'UngueltigerTyp'

        with self.assertRaises(DatabaseError, msg="Einfügen mit ungültigem ENUM sollte fehlschlagen"):
            self.db.insert_data(df_main, df_stars)

        self.cursor.execute("SELECT COUNT(*) FROM planets")
        self.assertEqual(self.cursor.fetchone()[0], 0,
                         "Nach fehlgeschlagenem ENUM-Insert sollte die Planetentabelle leer sein (Rollback)")

    def test_08_generated_column_on_update(self):
        """Tests if the generated visualization_url updates on pl_name UPDATE."""
        df_main, df_stars = self._create_dummy_data()
        self.db.insert_data(df_main, df_stars)

        self.cursor.execute("SELECT visualization_url FROM planets WHERE pl_name = 'TestPlanet A'")
        url_before = self.cursor.fetchone()[0]
        self.assertTrue(url_before.endswith("TestPlanet_A"))

        self.cursor.execute("UPDATE planets SET pl_name = 'TestPlanet A Neu' WHERE pl_name = 'TestPlanet A'")
        self.db.connection.commit()

        self.cursor.execute("SELECT visualization_url FROM planets WHERE pl_name = 'TestPlanet A Neu'")
        url_after = self.cursor.fetchone()[0]
        self.assertIsNotNone(url_after, "Planet sollte nach Umbenennung gefunden werden")
        self.assertTrue(url_after.endswith("TestPlanet_A_Neu"), "Generierte URL sollte nach UPDATE aktualisiert sein")


if __name__ == '__main__':
    unittest.main()