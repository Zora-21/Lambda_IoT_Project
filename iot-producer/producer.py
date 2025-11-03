#/Users/matteo/Documents/Lambda_IoT/iot-producer/producer.py

import os
import time
import json
import random
import logging
from datetime import datetime
from cassandra.cluster import Cluster
from hdfs import InsecureClient

# Impostazione del logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Variabili di Connessione ---
CASSANDRA_HOST = 'cassandra-seed'
CASSANDRA_KEYSPACE = 'iot_keyspace' # <-- CORREZIONE
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_DIR = '/iot-data'
HDFS_FILE = f"{HDFS_DIR}/sensor_logs.jsonl" # Unico file per i log

def get_cassandra_session():
    """Si connette al cluster Cassandra con una politica di re-try."""
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=9042)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            log.info("Connesso a Cassandra!")
            return session, cluster
        except Exception as e:
            log.warning(f"Attesa per Cassandra... ({e})")
            time.sleep(5)

def get_hdfs_client():
    """Si connette a HDFS e assicura che dir E file esistano."""
    while True:
        try:
            client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
            
            # 1. Assicurati che la directory di base esista
            if not client.status(HDFS_DIR, strict=False):
                 log.info(f"Creazione directory HDFS: {HDFS_DIR}")
                 client.makedirs(HDFS_DIR)
            
            # 2. NUOVA CORREZIONE: Assicurati che il FILE esista
            # Altrimenti 'append=True' fallisce.
            if not client.status(HDFS_FILE, strict=False):
                log.info(f"Creazione file HDFS vuoto: {HDFS_FILE}")
                # Scriviamo una stringa vuota per creare il file
                with client.write(HDFS_FILE, encoding='utf-8', append=False) as writer:
                    writer.write("") 
            
            log.info("Connesso a HDFS e file di log verificato!")
            return client
        except Exception as e:
            log.warning(f"Attesa per HDFS... ({e})")
            time.sleep(5)

def generate_sensor_data():
    """Genera un singolo dato (simulato)."""
    sensor_ids = ['A1', 'B1', 'C1']
    sensor = random.choice(sensor_ids)
    temp = round(random.uniform(18.0, 25.0), 2)
    timestamp = datetime.now()
    
    return {
        "sensor_id": sensor,
        "timestamp": timestamp,
        "temp": temp
    }

def main():
    log.info("Avvio del producer IoT...")
    
    cassandra_session, cassandra_cluster = get_cassandra_session()
    hdfs_client = get_hdfs_client()

    cassandra_query = cassandra_session.prepare(
        f"INSERT INTO sensor_data (sensor_id, timestamp, temp) VALUES (?, ?, ?)"
    )

    log.info("Inizio invio dati...")
    
    try:
        while True:
            data = generate_sensor_data()
            log.info(f"Dato generato: {data}")

            # 4. Invio allo Speed Layer (Cassandra)
            try:
                cassandra_session.execute(
                    cassandra_query, 
                    (data['sensor_id'], data['timestamp'], data['temp'])
                )
            except Exception as e:
                log.error(f"Errore scrittura Cassandra: {e}")

            # 5. Invio al Batch Layer (HDFS)
            try:
                data_hdfs = data.copy()
                data_hdfs['timestamp'] = data_hdfs['timestamp'].isoformat()
                json_data = json.dumps(data_hdfs) + '\n'
                
                # --- CORREZIONE LOGICA DI SCRITTURA HDFS ---
                # Rimuoviamo 'is_first_write'. Usiamo sempre 'append=True'.
                # InsecureClient crea il file se non esiste quando append=True.
                with hdfs_client.write(HDFS_FILE, encoding='utf-8', append=True) as writer:
                    writer.write(json_data)
                # ----------------------------------------
                    
            except Exception as e:
                log.error(f"Errore scrittura HDFS: {e}")

            time.sleep(2)
            
    except KeyboardInterrupt:
        log.info("Spegnimento producer...")
    finally:
        cassandra_session.shutdown()
        cassandra_cluster.shutdown()

if __name__ == "__main__":
    main()