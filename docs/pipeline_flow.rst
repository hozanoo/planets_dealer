:orphan:

Pipeline-Ablauf (`run_pipeline`)
================================

Dieses Dokument beschreibt den vollständigen Ablauf der ETL-Pipeline
ausgehend von der Funktion ``run_pipeline``.

Überblick
---------

Die Pipeline besteht aus vier Hauptphasen:

1. Extract
2. Transform (Merge)
3. Transform (Scraping)
4. Load (Datenbank)

Detailstruktur
--------------

.. code-block:: text

    run_pipeline(limit)

      ├─ 0. Setup
      │    ├─ BASE_DIR bestimmen (Projekt-Root)
      │    └─ .env laden (DB_NAME, DB_USER, ...)

      ├─ 1. EXTRACT
      │    ├─ df_nasa = fetch_exoplanets(limit)
      │    │     └─ (in api_logger)
      │    │          ├─ SQL-Query (PSCompPars)
      │    │          ├─ requests.get(...)  → NASA TAP API
      │    │          └─ CSV → DataFrame mit umbenannten Spalten
      │    │
      │    ├─ df_local = load_local_data(hwc.csv)
      │    │     └─ (in local_loader)
      │    │          ├─ pd.read_csv('data/hwc.csv')
      │    │          ├─ prüfen, ob alle erwarteten Spalten vorhanden sind
      │    │          └─ Spalten auswählen & umbenennen → Ergänzungs-DataFrame
      │    │
      │    └─ df_stars_api = fetch_stellar_hosts()
      │          └─ (in api_logger)
      │               ├─ SQL-Query (stellarhosts mit GROUP BY)
      │               ├─ requests.get(...)  → NASA TAP API
      │               └─ CSV → DataFrame mit umbenannten Spalten (system_key, star_name, ...)

      ├─ 2. TRANSFORM (MERGE)
      │    ├─ prüfen: hat df_nasa eine 'pl_name'-Spalte?
      │    ├─ df_nasa['pl_name_norm'] erzeugen
      │    │     └─ pl_name → String, lower, Leerzeichen/Minus entfernen
      │    │
      │    ├─ df_merged = df_nasa   (Basis sind immer NASA-Daten)
      │    │
      │    ├─ wenn df_local NICHT leer und 'pl_name_local' vorhanden:
      │    │     ├─ df_local['pl_name_norm'] aus pl_name_local bauen
      │    │     ├─ cols_to_merge bestimmen:
      │    │     │     └─ nur Spalten aus df_local, die es in df_nasa noch nicht gibt
      │    │     ├─ df_merged = merge(df_nasa, df_local[cols_to_merge],
      │    │     │                    on='pl_name_norm', how='left')
      │    │     └─ (sonst: df_merged bleibt = df_nasa)
      │    │
      │    └─ pl_name_norm wieder aus df_merged löschen

      ├─ 3. TRANSFORM (SCRAPING)
      │    └─ df_enriched = add_planet_type(df_merged)
      │          └─ (in web_logger)
      │               ├─ prüfen: DataFrame leer oder 'pl_name' fehlt?
      │               │     └─ dann: df unverändert zurück
      │               │
      │               ├─ alle Planetennamen sammeln (Set aus df['pl_name'])
      │               │
      │               ├─ planet_cache = load_cache(CACHE_FILE)
      │               │     └─ CSV 'planet_type_cache.csv' lesen
      │               │         → Dict {pl_name -> planet_type}
      │               │
      │               ├─ missing_planets bestimmen:
      │               │     └─ all_planet_names - planet_cache.keys()
      │               │
      │               ├─ wenn missing_planets NICHT leer:
      │               │     ├─ für jeden fehlenden Planeten:
      │               │     │     └─ planet_type = get_nasa_planet_type(name)
      │               │     │            └─ (HTTP-Request zu science.nasa.gov,
      │               │     │                HTML parsen, Text durchsuchen)
      │               │     ├─ newly_scraped_types in planet_cache einfügen
      │               │     └─ save_cache(CACHE_FILE, planet_cache)
      │               │           └─ Dict → DataFrame → CSV schreiben
      │               │
      │               ├─ df_copy['planet_type'] = df_copy['pl_name'].map(planet_cache)
      │               │     └─ fehlende → 'Unknown'
      │               └─ df_copy zurückgeben → df_enriched

      └─ 4. LOAD (in die PostgreSQL-Datenbank)
           └─ save_normalized_to_db(df_enriched, df_stars_api)
                 └─ (in save_data)
                      ├─ with ExoplanetDBPostgres() as db:
                      │     └─ __init__
                      │           └─ psycopg2.connect(...) mit ENV-Variablen
                      │              → self.connection, self.cursor
                      │
                      └─ db.insert_data(df_enriched, df_stars_api)
                            ├─ _recreate_tables()
                            │     ├─ DROP TABLE ... (planets, stars, systems, lookups)
                            │     ├─ DROP TYPE planet_type_enum
                            │     ├─ CREATE TYPE planet_type_enum (...)
                            │     ├─ CREATE TABLE discovery_facilities
                            │     ├─ CREATE TABLE detection_methods
                            │     ├─ CREATE TABLE systems
                            │     ├─ CREATE TABLE stars
                            │     └─ CREATE TABLE planets (mit generated visualization_url)
                            │
                            ├─ method_map, facility_map = _insert_lookup_data(df_enriched)
                            │     ├─ einzigartige detection_method_name aus df_enriched
                            │     │     └─ INSERT INTO detection_methods ... ON CONFLICT DO NOTHING
                            │     ├─ einzigartige facility_name aus df_enriched
                            │     │     └─ INSERT INTO discovery_facilities ... ON CONFLICT DO NOTHING
                            │     └─ danach SELECT ... → Dicts
                            │           ├─ method_map   = {name -> method_id}
                            │           └─ facility_map = {name -> facility_id}
                            │
                            ├─ Systems vorbereiten
                            │     ├─ df_systems = df_enriched mit 'star_name_api' → 'system_key'
                            │     ├─ nur Spalten:
                            │     │     system_key, constellation_en, distance_pc, HZ-Felder
                            │     ├─ Duplikate pro system_key entfernen
                            │     ├─ Zeilen ohne system_key entfernen
                            │     └─ system_tuples = _dataframe_to_tuples(df_systems)
                            │
                            ├─ Stars vorbereiten
                            │     ├─ df_stars = df_stars_api[star_name, system_key, st_teff, ...]
                            │     ├─ nur Sterne, deren system_key in df_systems vorkommt
                            │     ├─ Duplikate pro star_name entfernen
                            │     ├─ Zeilen ohne star_name entfernen
                            │     └─ star_tuples = _dataframe_to_tuples(df_stars)
                            │
                            ├─ Planets vorbereiten
                            │     ├─ df_planets_prep = df_enriched mit 'star_name_api' → 'star_name'
                            │     ├─ df_planets_prep['method_id']   = map(detection_method_name → method_id)
                            │     ├─ df_planets_prep['facility_id'] = map(facility_name → facility_id)
                            │     ├─ nur Spalten:
                            │     │     pl_name, star_name, method_id, facility_id,
                            │     │     planet_type, discovery_year, orbital_period_days, ...
                            │     ├─ Duplikate pro pl_name entfernen
                            │     ├─ Zeilen ohne pl_name entfernen
                            │     ├─ nur Planeten, deren star_name in df_stars vorkommt
                            │     └─ planet_tuples = _dataframe_to_tuples(df_planets)
                            │
                            ├─ In DB einfügen (bulk)
                            │     ├─ _execute_values("INSERT INTO systems ... ON CONFLICT DO NOTHING", system_tuples)
                            │     ├─ _execute_values("INSERT INTO stars   ... ON CONFLICT DO NOTHING", star_tuples)
                            │     └─ _execute_values("INSERT INTO planets ... ON CONFLICT DO NOTHING", planet_tuples)
                            │
                            ├─ self.connection.commit()
                            └─ beim Verlassen des with-Blocks:
                                  └─ __exit__ → close_connection()
                                         ├─ cursor.close()
                                         └─ connection.close()
