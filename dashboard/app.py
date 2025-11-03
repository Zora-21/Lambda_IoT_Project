#/Users/matteo/Documents/Lambda_IoT/dashboard/app.py

import logging
import time
from flask import Flask, render_template, jsonify
from cassandra.cluster import Cluster
from hdfs import InsecureClient # <-- IMPORTA HDFS

# Impostazione del logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO)

# --- Variabili di Connessione ---
CASSANDRA_HOST = 'cassandra-seed'
CASSANDRA_KEYSPACE = 'iot_keyspace' # <-- CORREZIONE
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_OUTPUT_FILE = '/iot-output/daily-averages/part-00000' # Risultato MapReduce

app = Flask(__name__)

# Variabili globali per le sessioni
cassandra_session = None
hdfs_client = None

@app.before_request 
def setup_connections():
    """Si connette a Cassandra e HDFS con una politica di re-try."""
    global cassandra_session, hdfs_client
    
    # Connettiti a Cassandra (se non già connesso)
    if not cassandra_session:
        while True:
            try:
                cluster = Cluster([CASSANDRA_HOST], port=9042)
                cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
                log.info("Dashboard: Connesso a Cassandra!") 
                break # Esce dal loop di Cassandra
            except Exception as e:
                log.warning(f"Dashboard: Attesa per Cassandra... ({e})")
                time.sleep(5)

    # Connettiti a HDFS (se non già connesso)
    if not hdfs_client:
        while True:
            try:
                hdfs_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
                # Fai un test di connessione
                hdfs_client.status('/') 
                log.info("Dashboard: Connesso a HDFS!")
                break # Esce dal loop di HDFS
            except Exception as e:
                log.warning(f"Dashboard: Attesa per HDFS... ({e})")
                time.sleep(5)

@app.route('/')
def index():
    """Renderizza la pagina HTML principale."""
    return render_template('index.html')

@app.route('/data/realtime')
def get_realtime_data():
    """Fornisce i dati più recenti (Speed Layer) come API JSON."""
    global cassandra_session
    if not cassandra_session:
        log.error("La sessione di Cassandra non è inizializzata!")
        return jsonify({"error": "Cassandra session not initialized"}), 500

    sensor_ids = ['A1', 'B1', 'C1']
    latest_data = {}

    for sensor_id in sensor_ids:
        try:
            query = "SELECT temp, timestamp FROM sensor_data WHERE sensor_id = %s LIMIT 1"
            row = cassandra_session.execute(query, [sensor_id]).one()
            
            if row:
                latest_data[sensor_id] = {
                    "temp": round(row.temp, 2),
                    "timestamp": row.timestamp.isoformat()
                }
            else:
                latest_data[sensor_id] = {"temp": "N/A", "timestamp": "N/A"}
        
        except Exception as e:
            log.error(f"Errore query Cassandra per {sensor_id}: {e}")
            latest_data[sensor_id] = {"error": str(e)}

    return jsonify(latest_data)

@app.route('/data/batch')
def get_batch_data():
    """Fornisce i dati storici (Batch Layer) leggendo da HDFS."""
    global hdfs_client
    if not hdfs_client:
        log.error("Il client HDFS non è inizializzato!")
        return jsonify({"error": "HDFS client not initialized"}), 500

    batch_data = {}
    try:
        # Controlla se il file di output del job MapReduce esiste
        status = hdfs_client.status(HDFS_OUTPUT_FILE, strict=False)
        
        if status:
            # Se esiste, leggilo riga per riga
            with hdfs_client.read(HDFS_OUTPUT_FILE, encoding='utf-8') as reader:
                for line in reader:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # L'output del reducer è: CHIAVE \t VALORE_MEDIO
                    key, avg_temp = line.split('\t', 1)
                    batch_data[key] = float(avg_temp)
            
            return jsonify(batch_data)
        else:
            # Il file non esiste, probabilmente il job non è ancora stato eseguito
            return jsonify({"status": "Batch job results not found. Run the MapReduce job."})

    except Exception as e:
        log.error(f"Errore lettura dati HDFS: {e}")
        return jsonify({"error": f"Failed to read HDFS data: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)