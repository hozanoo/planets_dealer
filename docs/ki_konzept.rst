=================
KI-Modell Konzept
=================

Motivation und Ziel
-------------------
Dieses Projekt zielt darauf ab, ein **KI-Modell vorzuschlagen**, das fehlende oder inkonsistente astrophysikalische Parameter von Exoplaneten schätzen kann – insbesondere **Masse, Radius und Temperatur**.
Diese Werte sind entscheidend für die Berechnung des **Earth Similarity Index (ESI)** und der **habitablen Zone (HZ)**, fehlen jedoch häufig in den astronomischen Datensätzen oder wurden nach unterschiedlichen wissenschaftlichen Methoden ermittelt.

Das **vorgeschlagene Modellkonzept** soll also eine **physikalisch konsistente Imputation** ermöglichen:
Es **soll in der Lage sein**, fehlende Werte zu ergänzen, ohne bekannte physikalische Zusammenhänge (z. B. Masse–Radius–Beziehung) zu verletzen.
Dadurch **könnten** vollständigere und konsistentere Bewertungen der Planet Habitability (Bewohnbarkeit) erzeugt werden.

.. tab-set::

   .. tab-item:: Methode 1 – Physikalisch konsistentes neuronales Netz
      :sync: m1

      Für diese Aufgabe bietet sich ein **neuronales Netz (NN)** mit optionalen *physics-informed*-Anteilen (PINN) an.
      Die Idee eines **PINN** ist, dass nicht nur der klassische Datenfehler (z. B. mittlere Abweichung) im Training berücksichtigt wird,
      sondern auch physikalische Gleichungen direkt in die **Loss-Funktion** integriert werden.

      Die Loss-Funktion **könnte** zum Beispiel so aussehen:

      .. math::

         \mathcal{L} =
         \underbrace{\|y_{\text{pred}} - y_{\text{true}}\|^2}_{\text{Datenfehler}}
         + \lambda_1(\rho - M/R^3)^2
         + \lambda_2(v_e - \sqrt{M/R})^2
         + \lambda_3(ESI_{\text{pred}} - ESI_{\text{phys}})^2

      Dabei **würden** die zusätzlichen Terme für physikalische Konsistenz sorgen:

      * :math:`\rho = M / R^3` (Dichte)
      * :math:`v_e = \sqrt{M / R}` (Fluchtgeschwindigkeit)
      * :math:`ESI = (S(R)\,S(\rho)\,S(v_e)\,S(T))^{1/4}` (Gesamtindex)

      Das Gewicht :math:`\lambda_i` **würde bestimmen**, wie stark die Physikbedingungen gegenüber dem Datenfehler gewichtet werden.

      Die **vorgesehenen** Trainingsdaten stammen aus mehreren Quellen und sind in der ETL-Pipeline bereits zusammengeführt worden:

      * **NASA Exoplanet Archive (PSCompPars, Stellarhosts)** — physikalische und orbitale Parameter:
        ``planet_radius_earth_radii``, ``planet_mass_earth_masses``, ``equilibrium_temperature_k``, ``st_teff``, ``st_lum`` usw.
      * **PHL (Planetary Habitability Laboratory)** – CSV-Datei mit habitablen Zonen und ESI-Werten:
        ``hz_conservative_inner_au``, ``hz_conservative_outer_au``, ``hz_optimistic_inner_au``, ``hz_optimistic_outer_au``, ``esi``.

      ---

      **Dashboard 1 – Habitability Comparison**

      Dieses Dashboard vergleicht den **habitablen Zonen-Status (HZ)** aus der PHL-CSV mit dem **neu berechneten HZ-Status**,
      der aus Sternleuchtkraft und Datenbank-Funktionen abgeleitet wurde.

      .. math::

         d_{\text{HZ}} = \frac{\sqrt{L}}{S_{\text{eff}}}

      * :math:`S_{\text{eff, inner}} = 1.1` (konservativ innen)
      * :math:`S_{\text{eff, outer}} = 0.53` (konservativ außen)
      * :math:`S_{\text{eff, opt, inner}} = 1.78`, :math:`S_{\text{eff, opt, outer}} = 0.4` (optimistisch)

      .. figure:: images/dashboard_hz_comparison.png
         :width: 80%
         :alt: Habitability Comparison Dashboard

      Abweichungen zwischen CSV- und berechneten HZ-Klassifikationen zeigen Daten-Inkonsistenzen – **die Motivation für eine KI-basierte Korrektur.**

      ---

      **Dashboard 2 – ESI Data Completeness**

      Das zweite Dashboard untersucht die **Vollständigkeit der ESI-Eingaben**.

      .. math::

         S(x) = \left(1 - \left|\frac{x - x_\oplus}{x + x_\oplus}\right|\right)^w, \qquad
         ESI = (S(R)\,S(\rho)\,S(v_e)\,S(T))^{1/4}

      :math:`w_R = 0.57`, :math:`w_\rho = 1.07`, :math:`w_{v_e} = 0.70`, :math:`w_T = 5.58`.

      .. math::

         \rho = M / R^3, \qquad v_e = \sqrt{M / R}

      .. figure:: images/dashboard_esi_completeness.png
         :width: 80%
         :alt: ESI Data Completeness Dashboard

      Dieses Dashboard zeigt, dass viele Datensätze mindestens einen fehlenden ESI-Parameter haben – eine optimale **Anwendungsgrundlage** für KI-basierte Schätzungen.

      ---

      **Dashboard 3 – Bias und Beobachtungsmethode**

      Das dritte Dashboard untersucht die **methodenabhängigen Datenlücken**:

      * **Transit** → misst Radius präzise, Masse fehlt oft.
      * **Radial Velocity** → liefert Masse, aber keine Temperatur.
      * **Microlensing** → unvollständige Radiusdaten.

      .. figure:: images/dashboard_method_bias.png
         :width: 80%
         :alt: Method Bias Dashboard

      Diese Analyse zeigt, dass die **Beobachtungsmethode selbst ein Bias-Faktor** ist.
      Daher **sollten** zusätzlich methodische Merkmale (`method_name`, `facility_name`, `discovery_year`) **für das Training berücksichtigt werden**,
      um Bias-Effekte zu lernen und auszugleichen.

      ---

      **Zusammenfassung**

      1. Dashboard 1 → Inkonsistenzen zwischen HZ-Berechnungen.
      2. Dashboard 2 → fehlende ESI-Daten.
      3. Dashboard 3 → methodische Biases.

      → **Dies begründet den Vorschlag** für ein erklärbares, physik-informiertes KI-Modell zur konsistenten Bewohnbarkeitsbewertung.

   .. tab-item:: Methode 2 – Platzhalter für weitere Ideen
      :sync: m2

      Dieser Tab dient als **Platzhalter für zukünftige Ansätze**.
      Beispielhaft **könnten** hier später alternative Methoden ergänzt werden – z. B.:

      * ein klassifikationsbasiertes Modell, das den HZ-Status direkt vorhersagt,
      * oder eine Bayes-basierte Unsicherheitsabschätzung für geschätzte Parameter.

      Zurzeit bleibt dieser Tab unbearbeitet und wird in einer späteren Projektphase ergänzt.