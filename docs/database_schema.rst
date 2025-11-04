Datenbank-Schema & SQL
======================

Hier ist eine Übersicht der SQL-Objekte.

.. tab-set::

   .. tab-item:: Tabellenübersicht
      :sync: t1

      **Übersicht der zentralen Tabellen**

      * **systems**: Speichert Systemdaten, Entfernungen und HZ-Grenzen.
      * **stars**: Speichert Sternparameter wie Temperatur, Leuchtkraft, Alter.
      * **planets**: Planetendaten, Orbitinformationen, physikalische Werte.
      * **detection_methods**: Lookup-Tabelle der Entdeckungsmethoden.
      * **discovery_facilities**: Lookup-Tabelle der Observatorien.

      **Logisches Schema**

      .. figure:: images/db_logical_schema.png
         :width: 90%
         :align: center
         :alt: Logisches Datenbankschema

         Abbildung: logisches Schema der relationalen Tabellenstruktur.

   .. tab-item:: SQL-Funktionen
      :sync: t2

      **Physikalische und analytische SQL-Funktionen**

      *Hier sind alle Funktionen zusammengefasst, die physikalische Größen berechnen oder Daten kategorisieren.*

      **1️⃣ Planeten-Dichte berechnen**

      Berechnet die Dichte eines Planeten basierend auf Radius und Masse in Erdbasiseinheiten.

      .. math::

         \rho = \frac{M_\text{planet}}{\frac{4}{3}\pi R_\text{planet}^3}

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION calculate_planet_density(radius_earth REAL, mass_earth REAL)
         RETURNS REAL AS $$
         DECLARE
             earth_radius_m CONSTANT REAL := 6371000.0;
             earth_mass_kg CONSTANT REAL := 5.972e24;
             planet_radius_m REAL;
             planet_mass_kg REAL;
             planet_volume_m3 REAL;
         BEGIN
             IF radius_earth IS NULL OR mass_earth IS NULL OR radius_earth <= 0 OR mass_earth <= 0 THEN
                 RETURN NULL;
             END IF;
             planet_radius_m := radius_earth * earth_radius_m;
             planet_mass_kg := mass_earth * earth_mass_kg;
             planet_volume_m3 := (4.0/3.0) * PI() * power(planet_radius_m, 3);
             RETURN planet_mass_kg / planet_volume_m3;
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

      **2️⃣ Parsec in Lichtjahre umrechnen**

      .. math::

         d_\text{ly} = d_\text{pc} \times 3.26156

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION convert_pc_to_ly(distance_parsec REAL)
         RETURNS REAL AS $$
         DECLARE
             pc_to_ly_conversion_factor CONSTANT REAL := 3.26156;
         BEGIN
             IF distance_parsec IS NULL THEN
                 RAISE NOTICE 'Eingabe für Lichtjahr-Umrechnung ist NULL.';
                 RETURN NULL;
             END IF;
             RETURN distance_parsec * pc_to_ly_conversion_factor;
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

      **3️⃣ Sterne nach Temperatur klassifizieren**

      Klassifiziert Sterne grob nach Spektralklasse gemäß effektiver Temperatur.

      .. list-table::
         :header-rows: 1

         * - Temperatur (K)
           - Klasse
         * - >= 30000
           - O
         * - 10000–29999
           - B
         * - 7500–9999
           - A
         * - 6000–7499
           - F
         * - 5200–5999
           - G
         * - 3700–5199
           - K
         * - 2400–3699
           - M

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION classify_star_by_temp(temperature_k REAL)
         RETURNS TEXT AS $$
         BEGIN
             IF temperature_k IS NULL THEN
                RAISE NOTICE 'Temperatur für Klassifizierung ist NULL.';
                RETURN 'Unknown';
             END IF;
             IF temperature_k >= 30000 THEN RETURN 'O';
             ELSIF temperature_k >= 10000 THEN RETURN 'B';
             ELSIF temperature_k >= 7500 THEN RETURN 'A';
             ELSIF temperature_k >= 6000 THEN RETURN 'F';
             ELSIF temperature_k >= 5200 THEN RETURN 'G';
             ELSIF temperature_k >= 3700 THEN RETURN 'K';
             ELSIF temperature_k >= 2400 THEN RETURN 'M';
             ELSE RETURN 'Cooler than M';
             END IF;
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

      **4️⃣ Planeten-Reihenfolge bestimmen**

      Ermittelt die Ordnungszahl (1, 2, 3...) eines Planeten in seinem System, sortiert nach Abstand.

      .. code-block:: postgresql

        CREATE OR REPLACE FUNCTION get_planet_order_in_system(
            target_pl_name TEXT,
            target_star_name TEXT
        ) RETURNS INTEGER AS $$
        DECLARE
            planet_rank INTEGER;
        BEGIN
            IF target_pl_name IS NULL OR target_star_name IS NULL THEN
                RAISE NOTICE 'Planeten- oder Sternname für Ordnungszahl ist NULL.';
                RETURN NULL;
            END IF;
            SELECT rank_num INTO planet_rank
            FROM (
                SELECT
                    pl_name,
                    ROW_NUMBER() OVER (ORDER BY orbit_semi_major_axis_au ASC) as rank_num
                FROM planets
                WHERE star_name = target_star_name
            ) ranked_planets
            WHERE pl_name = target_pl_name;
            RETURN planet_rank;
        END;
        $$ LANGUAGE plpgsql STABLE;

      **5️⃣ Habitable-Zonen-Berechnung (Vereinfacht)**

      Berechnet vereinfachte HZ-Grenzen basierend auf Sternleuchtkraft (L) und Strahlungsfluss (S_eff).

      .. math::

         d_{\text{HZ}} = \frac{\sqrt{L/L_\text{Sonne}}}{S_\text{eff}}

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION _calculate_hz_distance(lum_sol REAL, seff_limit REAL)
         RETURNS REAL AS $$
         BEGIN
             IF lum_sol IS NULL OR lum_sol < 0 OR seff_limit IS NULL OR seff_limit <= 0 THEN
                 RETURN NULL;
             END IF;
             RETURN (sqrt(lum_sol) / seff_limit)::REAL;
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

         CREATE OR REPLACE FUNCTION calculate_hz_conservative_inner(star_luminosity_logsol REAL)
         RETURNS REAL AS $$
         DECLARE lum_sol REAL; s_eff CONSTANT REAL := 1.1;
         BEGIN
             IF star_luminosity_logsol IS NULL THEN RETURN NULL; END IF;
             lum_sol := power(10, star_luminosity_logsol);
             RETURN _calculate_hz_distance(lum_sol, s_eff);
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

         CREATE OR REPLACE FUNCTION calculate_hz_conservative_outer(star_luminosity_logsol REAL)
         RETURNS REAL AS $$
         DECLARE lum_sol REAL; s_eff CONSTANT REAL := 0.53;
         BEGIN
             IF star_luminosity_logsol IS NULL THEN RETURN NULL; END IF;
             lum_sol := power(10, star_luminosity_logsol);
             RETURN _calculate_hz_distance(lum_sol, s_eff);
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

         CREATE OR REPLACE FUNCTION calculate_hz_optimistic_inner(star_luminosity_logsol REAL)
         RETURNS REAL AS $$
         DECLARE lum_sol REAL; s_eff CONSTANT REAL := 1.78;
         BEGIN
             IF star_luminosity_logsol IS NULL THEN RETURN NULL; END IF;
             lum_sol := power(10, star_luminosity_logsol);
             RETURN _calculate_hz_distance(lum_sol, s_eff);
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

         CREATE OR REPLACE FUNCTION calculate_hz_optimistic_outer(star_luminosity_logsol REAL)
         RETURNS REAL AS $$
         DECLARE lum_sol REAL; s_eff CONSTANT REAL := 0.4;
         BEGIN
             IF star_luminosity_logsol IS NULL THEN RETURN NULL; END IF;
             lum_sol := power(10, star_luminosity_logsol);
             RETURN _calculate_hz_distance(lum_sol, s_eff);
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

      **6️⃣ HZ-Status bestimmen (CSV vs. Berechnet)**

      Vergleicht Orbit eines Planeten mit HZ-Grenzen aus CSV oder berechneten Werten.

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION check_hz_status_csv(
             planet_orbit_au REAL,
             hz_con_inner REAL, hz_con_outer REAL,
             hz_opt_inner REAL, hz_opt_outer REAL)
         RETURNS TEXT AS $$
         BEGIN
             IF planet_orbit_au IS NULL OR hz_con_inner IS NULL THEN
                RAISE NOTICE 'Eingabe für HZ-Status (CSV) ist NULL.';
                RETURN 'Unknown';
             END IF;
             IF planet_orbit_au BETWEEN hz_con_inner AND hz_con_outer THEN RETURN 'Conservative';
             ELSIF planet_orbit_au BETWEEN hz_opt_inner AND hz_opt_outer THEN RETURN 'Optimistic';
             ELSE RETURN 'Outside';
             END IF;
         END;
         $$ LANGUAGE plpgsql IMMUTABLE;

      .. code-block:: postgresql

         CREATE OR REPLACE FUNCTION check_hz_status_calculated(
             planet_orbit_au REAL, star_luminosity_logsol REAL)
         RETURNS TEXT AS $$
         DECLARE
             calc_con_inner REAL; calc_con_outer REAL;
             calc_opt_inner REAL; calc_opt_outer REAL;
         BEGIN
             IF planet_orbit_au IS NULL OR star_luminosity_logsol IS NULL THEN
                 RAISE NOTICE 'Eingabe für HZ-Status (berechnet) ist NULL.';
                 RETURN 'Unknown (Input Missing)';
             END IF;
             calc_con_inner := calculate_hz_conservative_inner(star_luminosity_logsol);
             calc_con_outer := calculate_hz_conservative_outer(star_luminosity_logsol);
             calc_opt_inner := calculate_hz_optimistic_inner(star_luminosity_logsol);
             calc_opt_outer := calculate_hz_optimistic_outer(star_luminosity_logsol);

             IF calc_con_inner IS NULL THEN
                RETURN 'Unknown (HZ Calc Failed)';
             END IF;

             IF planet_orbit_au BETWEEN calc_con_inner AND calc_con_outer THEN RETURN 'Conservative';
             ELSIF planet_orbit_au BETWEEN calc_opt_inner AND calc_opt_outer THEN RETURN 'Optimistic';
             ELSE RETURN 'Outside';
             END IF;
         END;
         $$ LANGUAGE plpgsql STABLE;

      **7️⃣ Zählfunktionen**

      Zählt Planeten pro Einrichtung oder Methode.

      .. code-block:: postgresql

        CREATE OR REPLACE FUNCTION get_facility_planet_count(target_facility_id INTEGER)
        RETURNS INTEGER AS $$
        DECLARE
            p_count INTEGER;
        BEGIN
            IF target_facility_id IS NULL THEN
                RAISE NOTICE 'Facility ID ist NULL.';
                RETURN 0;
            END IF;
            SELECT COUNT(*) INTO p_count FROM planets WHERE facility_id = target_facility_id;
            RETURN p_count;
        END;
        $$ LANGUAGE plpgsql STABLE;

      .. code-block:: postgresql

        CREATE OR REPLACE FUNCTION get_method_planet_count(target_method_id INTEGER)
        RETURNS INTEGER AS $$
        DECLARE
            p_count INTEGER;
        BEGIN
            IF target_method_id IS NULL THEN
                RAISE NOTICE 'Method ID ist NULL.';
                RETURN 0;
            END IF;
            SELECT COUNT(*) INTO p_count FROM planets WHERE method_id = target_method_id;
            RETURN p_count;
        END;
        $$ LANGUAGE plpgsql STABLE;

   .. tab-item:: Views
      :sync: t3

      **Analytische Views**

      Diese Views kombinieren Daten aus mehreren Tabellen und rufen
      die obigen Funktionen auf, um komplexe Analysen zu ermöglichen.

      **1️⃣ View: v_planet_details_comparison**

      Kombiniert Planeten-, Stern-, System- und Entdeckungsdaten.
      Enthält Dichte, HZ-Status (CSV & berechnet), Sternklassifikation und Distanzumrechnung.

      .. code-block:: sql

         CREATE OR REPLACE VIEW v_planet_details_comparison AS
         SELECT
             -- Planet Info
             p.pl_name,
             p.planet_type,
             p.discovery_year,
             p.orbital_period_days,
             p.orbit_semi_major_axis_au,
             p.planet_radius_earth_radii,
             p.planet_mass_earth_masses,
             p.orbit_eccentricity,
             p.equilibrium_temperature_k,
             p.insolation_flux_earth_flux,
             p.esi,
             p.visualization_url,

             -- Calculated Planet Info
             calculate_planet_density(p.planet_radius_earth_radii, p.planet_mass_earth_masses) AS density_kgm3,
             get_planet_order_in_system(p.pl_name, p.star_name) AS planet_order_in_system,

             -- HZ Status Comparison
             check_hz_status_csv(
                 p.orbit_semi_major_axis_au,
                 sys.hz_conservative_inner_au, sys.hz_conservative_outer_au,
                 sys.hz_optimistic_inner_au, sys.hz_optimistic_outer_au
             ) AS hz_status_from_csv,
             check_hz_status_calculated(
                 p.orbit_semi_major_axis_au,
                 s.st_lum
             ) AS hz_status_calculated,

             -- Discovery Info
             dm.method_name AS detection_method,
             df.facility_name AS discovery_facility,

             -- Star Info
             s.star_name,
             s.st_teff AS star_temperature_k,
             classify_star_by_temp(s.st_teff) AS star_spectral_class_approx,
             s.st_lum AS star_luminosity_logsol,
             s.st_age AS star_age_gyr,
             s.st_met AS star_metallicity_dex,

             -- System Info
             sys.system_key,
             sys.constellation_en AS constellation,
             sys.distance_pc,
             convert_pc_to_ly(sys.distance_pc) AS distance_ly,
             sys.hz_conservative_inner_au,
             sys.hz_conservative_outer_au,
             sys.hz_optimistic_inner_au,
             sys.hz_optimistic_outer_au

         FROM
             planets p
         LEFT JOIN
             stars s ON p.star_name = s.star_name
         LEFT JOIN
             systems sys ON s.system_key = sys.system_key
         LEFT JOIN
             detection_methods dm ON p.method_id = dm.method_id
         LEFT JOIN
             discovery_facilities df ON p.facility_id = df.facility_id;


      **2️⃣ View: v_discovery_summary**

      Aggregiert ESI, Anzahl Planeten und durchschnittliche Entfernung
      pro Entdeckungsmethode und -einrichtung.

      .. code-block:: sql

         CREATE OR REPLACE VIEW v_discovery_summary AS
         SELECT
             dm.method_name,
             df.facility_name,
             COUNT(p.pl_name) AS number_of_planets,
             AVG(p.esi) AS average_esi,
             AVG(p.orbit_semi_major_axis_au) AS average_sma_au,
             AVG(sys.distance_pc) AS average_distance_pc
         FROM
             planets p
         LEFT JOIN
             detection_methods dm ON p.method_id = dm.method_id
         LEFT JOIN
             discovery_facilities df ON p.facility_id = df.facility_id
         LEFT JOIN
             stars s ON p.star_name = s.star_name
         LEFT JOIN
             systems sys ON s.system_key = sys.system_key
         GROUP BY
             dm.method_name,
             df.facility_name
         ORDER BY
             number_of_planets DESC;

      .. figure:: images/view_discovery_summary.png
         :width: 85%
         :alt: View Discovery Summary Schema