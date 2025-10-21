# 1. Wähle ein Basis-Image
FROM python:3.9

# 2. Setze ein Arbeitsverzeichnis im Container
WORKDIR /app

# 3. Kopiere die Abhängigkeiten-Datei
COPY requirements.txt .

# 4. Installiere die Abhängigkeiten
RUN pip install --no-cache-dir -r requirements.txt

# 5. Kopiere deinen gesamten Projektcode in den Container
COPY ./src ./src
COPY ./data ./data
COPY ./main.py .
COPY ./.env .

# 6. Definiere den Befehl, der beim Start ausgeführt wird
CMD ["python", "main.py"]