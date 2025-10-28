# src/save_data.py
import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from psycopg2.errors import UndefinedTable
from typing import List, Tuple, Dict, Optional


class DatabaseError(Exception): pass


class ExoplanetDBPostgres:
    """
    Handles all database interactions for the Exoplanet ETL pipeline.

    This class manages the database connection, schema creation (tables, ENUMs),
    and data insertion logic for the normalized 5-table schema.
    """

    def __init__(self):
        """Initializes the database connection and cursor."""
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        try:
            self.connection = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'), host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT', 5432)
            )
            self.cursor = self.connection.cursor()
            print(f"Successfully connected to DB '{os.environ.get('DB_NAME')}' on host '{os.environ.get('DB_HOST')}'.")
        except Exception as e:
            raise DatabaseError(f"Database connection failed: {e}")

    def _execute_values(self, query: str, data_tuples: List[Tuple], page_size: int = 100):
        """
        Executes a bulk INSERT statement using psycopg2's execute_values for efficiency.

        :param query: The SQL INSERT query template (e.g., "INSERT INTO table (...) VALUES %s").
        :type query: str
        :param data_tuples: A list of tuples containing the data to insert.
        :type data_tuples: List[Tuple]
        :param page_size: The number of rows to insert per network roundtrip.
        :type page_size: int
        """
        if not data_tuples: return
        if not self.cursor: raise DatabaseError("Cursor not available.")
        try:
            execute_values(self.cursor, query, data_tuples, page_size=page_size)
        except Exception as e:
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Error executing bulk insert: {e}\nQuery Template: {query[:500]}...")

    def _recreate_tables(self):
        """
        Drops all existing project tables, types, and recreates the 5-table schema.

        Schema:
        - discovery_facilities (Lookup)
        - detection_methods (Lookup)
        - systems (Parent)
        - stars (Child of systems)
        - planets (Child of stars, detection_methods, discovery_facilities)
        """
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        try:
            print("Resetting table schema (DROP/CREATE)...")
            self.cursor.execute("DROP TABLE IF EXISTS planets CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS stars CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS systems CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS detection_methods CASCADE;")
            self.cursor.execute("DROP TABLE IF EXISTS discovery_facilities CASCADE;")
            self.cursor.execute("DROP TYPE IF EXISTS planet_type_enum CASCADE;")

            self.cursor.execute(
                "CREATE TYPE planet_type_enum AS ENUM ('Neptune-like', 'terrestrial', 'gas giant', 'super Earth', 'Unknown');")
            self.cursor.execute(
                "CREATE TABLE discovery_facilities (facility_id SERIAL PRIMARY KEY, facility_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute(
                "CREATE TABLE detection_methods (method_id SERIAL PRIMARY KEY, method_name TEXT UNIQUE NOT NULL);")
            self.cursor.execute("""
                                CREATE TABLE systems
                                (
                                    system_key               TEXT PRIMARY KEY,
                                    constellation_en         TEXT,
                                    distance_pc              REAL,
                                    hz_conservative_inner_au REAL,
                                    hz_conservative_outer_au REAL,
                                    hz_optimistic_inner_au   REAL,
                                    hz_optimistic_outer_au   REAL
                                );""")
            self.cursor.execute("""
                                CREATE TABLE stars
                                (
                                    star_name  TEXT PRIMARY KEY,
                                    system_key TEXT REFERENCES systems (system_key) ON DELETE SET NULL,
                                    st_teff    REAL,
                                    st_lum     REAL,
                                    st_age     REAL,
                                    st_met     REAL
                                );""")
            self.cursor.execute("""
                                CREATE TABLE planets
                                (
                                    pl_name                    TEXT PRIMARY KEY,
                                    star_name                  TEXT    REFERENCES stars (star_name) ON DELETE SET NULL,
                                    method_id                  INTEGER REFERENCES detection_methods (method_id) ON DELETE SET NULL,
                                    facility_id                INTEGER REFERENCES discovery_facilities (facility_id) ON DELETE SET NULL,
                                    planet_type                planet_type_enum,
                                    discovery_year             INTEGER,
                                    orbital_period_days        REAL,
                                    orbit_semi_major_axis_au   REAL,
                                    planet_radius_earth_radii  REAL,
                                    planet_mass_earth_masses   REAL,
                                    orbit_eccentricity         REAL,
                                    equilibrium_temperature_k  REAL,
                                    insolation_flux_earth_flux REAL,
                                    esi                        REAL,
                                    visualization_url          TEXT GENERATED ALWAYS AS (
                                        'https://eyes.nasa.gov/apps/exo/#/planet/' || replace(pl_name, ' ', '_')
                                        ) STORED
                                );""")
            self.connection.commit()
            print("Tables created successfully.")
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Error creating tables: {e}")

    def _dataframe_to_tuples(self, df: pd.DataFrame) -> List[Tuple]:
        """
        Converts a Pandas DataFrame to a list of tuples, replacing NaN/NA with None.

        :param df: The DataFrame to convert.
        :type df: pd.DataFrame
        :return: A list of tuples.
        :rtype: List[Tuple]
        """
        df_clean: pd.DataFrame = df.astype(object).where(pd.notnull(df), None)
        df_clean = df_clean.replace({pd.NA: None})
        return [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

    def _insert_lookup_data(self, df_merged: pd.DataFrame) -> Tuple[Dict[str, int], Dict[str, int]]:
        """
        Populates the lookup tables (detection_methods, discovery_facilities)
        and returns dictionaries mapping names to their new IDs.

        :param df_merged: The merged DataFrame containing 'detection_method_name'
                          and 'facility_name' columns.
        :type df_merged: pd.DataFrame
        :return: A tuple of two dictionaries: (method_map, facility_map).
        :rtype: Tuple[Dict[str, int], Dict[str, int]]
        """
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        method_map: Dict[str, int] = {};
        facility_map: Dict[str, int] = {}
        try:
            if 'detection_method_name' in df_merged.columns:
                unique_methods = df_merged['detection_method_name'].dropna().unique()
                method_tuples: List[Tuple] = [(name,) for name in unique_methods]
                if method_tuples:
                    query = "INSERT INTO detection_methods (method_name) VALUES (%s) ON CONFLICT (method_name) DO NOTHING;"
                    self.cursor.executemany(query, method_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT method_name, method_id FROM detection_methods")
                    method_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(method_map)} detection methods inserted/found.")

            if 'facility_name' in df_merged.columns:
                unique_facilities = df_merged['facility_name'].dropna().unique()
                facility_tuples: List[Tuple] = [(name,) for name in unique_facilities]
                if facility_tuples:
                    query = "INSERT INTO discovery_facilities (facility_name) VALUES (%s) ON CONFLICT (facility_name) DO NOTHING;"
                    self.cursor.executemany(query, facility_tuples)
                    self.connection.commit()
                    self.cursor.execute("SELECT facility_name, facility_id FROM discovery_facilities")
                    facility_map = {name: id_ for name, id_ in self.cursor.fetchall()}
                    print(f"{len(facility_map)} discovery facilities inserted/found.")

            return method_map, facility_map
        except Exception as e:
            self.connection.rollback()
            raise DatabaseError(f"Error inserting lookup data: {e}")

    def insert_data(self, df_main_merged: pd.DataFrame, df_stars_api: pd.DataFrame):
        """
        Splits the provided DataFrames and inserts all data into the 5 normalized tables.

        This method orchestrates the entire load process:
        1. Recreates tables.
        2. Populates lookup tables.
        3. Prepares and filters data for systems, stars, and planets.
        4. Inserts data into main tables using bulk `execute_values`.
        5. Commits the transaction.

        :param df_main_merged: DataFrame containing merged data from PSCompPars and local CSV.
        :type df_main_merged: pd.DataFrame
        :param df_stars_api: DataFrame containing data from the stellarhosts API.
        :type df_stars_api: pd.DataFrame
        """
        if not self.cursor or not self.connection: raise DatabaseError("No DB connection.")
        if df_main_merged.empty or df_stars_api.empty:
            print("Warning: No data to insert.")
            return
        try:
            self._recreate_tables()
            method_map, facility_map = self._insert_lookup_data(df_main_merged)

            print("Preparing systems data...")
            df_systems: pd.DataFrame = df_main_merged.rename(columns={'star_name_api': 'system_key'})
            system_cols: List[str] = [
                'system_key', 'constellation_en', 'distance_pc', 'hz_conservative_inner_au',
                'hz_conservative_outer_au', 'hz_optimistic_inner_au', 'hz_optimistic_outer_au'
            ]
            valid_system_cols: List[str] = [col for col in system_cols if col in df_systems.columns]
            df_systems = df_systems[valid_system_cols].drop_duplicates(subset=['system_key']).dropna(
                subset=['system_key'])
            system_tuples: List[Tuple] = self._dataframe_to_tuples(df_systems)

            print("Preparing stars data...")
            star_cols: List[str] = ['star_name', 'system_key', 'st_teff', 'st_lum', 'st_age', 'st_met']
            valid_star_cols = [col for col in star_cols if col in df_stars_api.columns]
            df_stars: pd.DataFrame = df_stars_api[valid_star_cols].copy()
            df_stars = df_stars[df_stars['system_key'].isin(df_systems['system_key'])]
            df_stars = df_stars.drop_duplicates(subset=['star_name']).dropna(subset=['star_name'])
            star_tuples: List[Tuple] = self._dataframe_to_tuples(
                df_stars[valid_star_cols])  # Ensure correct column order

            print("Preparing planets data...")
            df_planets_prep: pd.DataFrame = df_main_merged.rename(columns={'star_name_api': 'star_name'})
            df_planets_prep['method_id'] = df_planets_prep['detection_method_name'].map(
                method_map) if 'detection_method_name' in df_planets_prep.columns else None
            df_planets_prep['facility_id'] = df_planets_prep['facility_name'].map(
                facility_map) if 'facility_name' in df_planets_prep.columns else None
            planet_cols: List[str] = [
                'pl_name', 'star_name', 'method_id', 'facility_id', 'planet_type', 'discovery_year',
                'orbital_period_days', 'orbit_semi_major_axis_au', 'planet_radius_earth_radii',
                'planet_mass_earth_masses', 'orbit_eccentricity', 'equilibrium_temperature_k',
                'insolation_flux_earth_flux', 'esi'
            ]
            valid_planet_cols: List[str] = [col for col in planet_cols if col in df_planets_prep.columns]
            df_planets: pd.DataFrame = df_planets_prep[valid_planet_cols]
            df_planets = df_planets.drop_duplicates(subset=['pl_name']).dropna(subset=['pl_name'])
            df_planets = df_planets[df_planets['star_name'].isin(df_stars['star_name'])]
            planet_tuples: List[Tuple] = self._dataframe_to_tuples(df_planets)

            print(f"Inserting {len(system_tuples)} systems...")
            cols_systems_str: str = ", ".join(valid_system_cols)
            query_systems: str = f"INSERT INTO systems ({cols_systems_str}) VALUES %s ON CONFLICT (system_key) DO NOTHING"
            self._execute_values(query_systems, system_tuples)

            print(f"Inserting {len(star_tuples)} stars...")
            cols_stars_str: str = ", ".join(valid_star_cols)
            query_stars: str = f"INSERT INTO stars ({cols_stars_str}) VALUES %s ON CONFLICT (star_name) DO NOTHING"
            self._execute_values(query_stars, star_tuples)

            print(f"Inserting {len(planet_tuples)} planets...")
            cols_planets_str: str = ", ".join(valid_planet_cols)
            query_planets: str = f"INSERT INTO planets ({cols_planets_str}) VALUES %s ON CONFLICT (pl_name) DO NOTHING"
            self._execute_values(query_planets, planet_tuples)

            self.connection.commit()
            print("Data successfully inserted into all tables.")
        except Exception as e:
            print(f"ERROR during data insertion: {e}")
            if self.connection: self.connection.rollback()
            raise DatabaseError(f"Transaction failed: {e}")

    def close_connection(self):
        """Closes the database cursor and connection."""
        try:
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing DB connection: {e}")
        finally:
            self.cursor = None;
            self.connection = None

    def __enter__(self):
        """Enables use of the 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures the connection is closed when exiting the 'with' block."""
        self.close_connection()


def save_normalized_to_db(df_main_merged: pd.DataFrame, df_stars_api: pd.DataFrame):
    """
    High-level wrapper function to save DataFrames into the database
    using the ExoplanetDBPostgres context manager.

    :param df_main_merged: DataFrame with planet and system data.
    :type df_main_merged: pd.DataFrame
    :param df_stars_api: DataFrame with star data.
    :type df_stars_api: pd.DataFrame
    """
    try:
        with ExoplanetDBPostgres() as db:
            db.insert_data(df_main_merged, df_stars_api)
    except (DatabaseError, ValueError, Exception) as e:
        print(f"Error saving data to database: {e}")
        raise