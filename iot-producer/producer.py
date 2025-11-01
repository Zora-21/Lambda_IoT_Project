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
CASSANDRA_KEYSPACE = 'moviedb'
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_PATH = '/iot-data/sensor_logs.jsonl' 

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
    """Si connette a HDFS."""
    while True:
        try:
            client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
            base_dir = os.path.dirname(HDFS_PATH)
            if not client.status(base_dir, strict=False):
                 client.makedirs(base_dir)
            log.info("Connesso a HDFS!")
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
    
    is_first_write = True 
    
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
                if is_first_write:
                    # La prima volta, sovrascrivi (o crea) il file
                    with hdfs_client.write(HDFS_PATH, encoding='utf-8', overwrite=True) as writer:
                        writer.write(json_data)
                    is_first_write = False # Le prossime volte appenderà
                    log.info(f"Creato file HDFS: {HDFS_PATH}")
                else:
                    # Le volte successive, aggiungi in coda (append)
                    with hdfs_client.write(HDFS_PATH, encoding='utf-8', append=True) as writer:
                        writer.write(json_data)
                # ----------------------------------------
                    
            except Exception as e:
                log.error(f"Errore scrittura HDFS: {e}")
                # Se la scrittura fallisce, riprova a creare il file la prossima volta
                is_first_write = True 

            time.sleep(2)
            
    except KeyboardInterrupt:
        log.info("Spegnimento producer...")
    finally:
        cassandra_session.shutdown()
        cassandra_cluster.shutdown()

if __name__ == "__main__":
    main()