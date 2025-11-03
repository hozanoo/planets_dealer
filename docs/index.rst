#####################################
Willkommen zur Exoplaneten-Pipeline
#####################################

Dieses Projekt wurde im Rahmen des Miniprojekts in der Ausbildung "Data Engineering & AI" bei Coders.Bay Vienna erstellt. Der Hauptzweck ist das praktische Üben einer vollständigen ETL-Pipeline unter Verwendung professioneller Software-Architektur.

Der Prozess umfasst:

* **Extract:** Abrufen von Daten aus externen Quellen (NASA APIs) und lokalen Dateien (CSV).
* **Transform:** Bereinigen der Daten, Anreicherung durch Web-Scraping und Aufbereitung für ein normalisiertes Datenbankschema.
* **Load:** Speichern der aufbereiteten Daten in einer PostgreSQL-Datenbank.
* **Analyse:** Ausführen von visuellen Analysen in Power BI.

Das gesamte Projekt wird mit Docker containerisiert, über Git (GitHub) versioniert und durch eine `README.md`-Datei sowie diese Sphinx-Dokumentation (als Eigenleistung) dokumentiert.

Da es sich um ein Miniprojekt mit begrenzter Zeit handelt, ist es als Proof-of-Concept zu verstehen. Viele Bereiche (z.B. Fehlerbehandlung, Testabdeckung, Anreicherungslogik) könnten weiter ausgebaut und optimiert werden.

**GitHub Repository:** `https://github.com/hozanoo/planets_dealer <https://github.com/hozanoo/planets_dealer>`_

---

Projektübersicht & Ausführung
===============================

Dieses Projekt lädt Exoplanetendaten von der NASA API und einer lokalen CSV-Datei, bereichert sie durch Web-Scraping und lädt das Ergebnis in eine normalisierte PostgreSQL-Datenbank (5-Tabellen-Schema).

Projektstruktur
-----------------

Der Code ist modular aufgebaut im ``src/`` Verzeichnis:

* ``api_logger.py``: Holt Daten von den NASA-APIs (Planeten und Sterne).
* ``local_loader.py``: Lädt die lokale ``hwc.csv``-Datei.
* ``web_logger.py``: Führt das Web-Scraping für Planetentypen durch (inkl. CSV-Caching in ``data/planet_type_cache.csv``).
* ``save_data.py``: Enthält die gesamte Datenbanklogik (Schema, Inserts).
* ``pipeline.py``: Orchestriert den gesamten ETL-Lauf.
* ``main.py``: Der Startpunkt des Projekts.
* ``tests/``: Enthält Integrationstests für die Datenbanklogik (``save_data.py``).

Ausführung (mit Docker)
-------------------------

1.  **``.env``-Datei erstellen:**
    Erstelle eine ``.env``-Datei (basierend auf ``env.example``). Stelle sicher, dass ``DB_HOST=db`` gesetzt ist (damit die Container die Docker-Datenbank finden).

2.  **Datenbank-Dienste starten:**
    Dieser Befehl baut die Images neu (falls nötig) und startet die ``db``- und ``pgadmin``-Container im Hintergrund.

    .. code-block:: bash

        docker-compose up --build -d db pgadmin

    *(Warte 15-20 Sekunden, bis die Datenbank bereit ist).*

3.  **(Optional) Integrationstests ausführen:**
    Dieser Befehl startet den ``tester``-Service, der eine separate Test-Datenbank erstellt, die ``save_data.py``-Logik prüft und die Test-Datenbank wieder löscht.

    .. code-block:: bash

        docker-compose run --rm tester

4.  **Haupt-ETL-Pipeline ausführen:**
    Dieser Befehl startet die ``pipeline`` (ruft ``main.py`` auf), die die finalen Tabellen erstellt und die echten Daten von den APIs und der CSV-Datei lädt.

    .. code-block:: bash

        docker-compose run --rm pipeline

    *(Hinweis: Beim ersten Lauf ist das Web-Scraping langsam. Zukünftige Läufe verwenden die Cache-Datei ``data/planet_type_cache.csv``).*

5.  **Daten ansehen:**
    * **pgAdmin:** Öffne ``http://localhost:8080`` (Login: ``admin@example.com`` / ``admin123``).
    * **DBeaver (oder lokal):** Verbinde dich mit ``localhost`` auf Port ``5433`` (wie in ``docker-compose.yml`` festgelegt).

Hinweis zum Web-Scraping
-------------------------

Das ``web_logger.py``-Modul greift auf ``science.nasa.gov`` zu. Das Scraping erfolgt langsam und respektvoll (``time.sleep``), um die Server nicht zu belasten, und die ``robots.txt``-Datei der Seite erlaubt das Crawlen des Pfades ``/exoplanet-catalog/``.

**Wartbarkeit:** Der Scraper ist vom aktuellen HTML-Layout (CSS-Selektor ``div.custom-field span``) der NASA-Seite abhängig. Sollte die NASA ihre Webseite ändern, muss dieser Selektor im Code angepasst werden.

---

.. toctree::
   :maxdepth: 2
   :caption: Module (Code-Referenz):

   api
   local_loader
   web_logger
   save_data
   pipeline

.. toctree::
   :maxdepth: 2
   :caption: Tests:

   tests

.. toctree::
   :maxdepth: 2
   :caption: Datenbank & KI-Konzept:

   database_schema
   ki_konzept