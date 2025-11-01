#/Users/matteo/Documents/Lambda_IoT/iot-producer/Dockerfile

# Partiamo da un'immagine Python 3.9 ufficiale e "slim" (leggera)
FROM python:3.9-slim

# Impostiamo una directory di lavoro all'interno del container
WORKDIR /app

# Copiamo prima il file dei requisiti
# (Trucco Docker: questo strato viene messo in cache e non rieseguito se il file non cambia)
COPY requirements.txt .

# Installiamo le dipendenze Python necessarie
# --no-cache-dir evita di salvare la cache, mantenendo l'immagine piccola
RUN pip install --no-cache-dir -r requirements.txt

# Copiamo il resto del codice sorgente del producer nel container
COPY . .

# Comando da eseguire quando il container viene avviato
# Avvia lo script 'producer.py'
CMD ["python", "producer.py"]