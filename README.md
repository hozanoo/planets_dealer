# Exoplaneten ETL-Pipeline

Dieses Projekt lädt Exoplanetendaten von der NASA API und einer lokalen CSV,
bereichert sie durch Scraping und lädt das Ergebnis in eine normalisierte
PostgreSQL-Datenbank.

## Ausführung (mit Docker)

1.  Erstelle eine `.env`-Datei (basierend auf `env.example`).
2.  Starte die Datenbank: `docker-compose up -d db pgadmin`
3.  Lasse die Pipeline laufen: `docker-compose run --rm pipeline`