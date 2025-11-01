#/Users/matteo/Documents/Lambda_IoT/dashboard/app.py
import logging
import time
from flask import Flask, render_template, jsonify
from cassandra.cluster import Cluster

# Impostazione del logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO)

# --- Variabili di Connessione ---
CASSANDRA_HOST = 'cassandra-seed'
CASSANDRA_KEYSPACE = 'moviedb'

app = Flask(__name__)

# Variabile globale per la sessione Cassandra
cassandra_session = None

# CORREZIONE: Usiamo il decoratore moderno @app.before_request
# Questo blocco viene eseguito prima di *ogni* richiesta.
@app.before_request 
def setup_cassandra():
    """Si connette al cluster Cassandra con una politica di re-try."""
    global cassandra_session
    if cassandra_session:
        return  # Già connesso, non fare nulla

    # Se non siamo connessi, proviamo a connetterci
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=9042)
            cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
            log.info("Dashboard: Connesso a Cassandra!") 
            return # Esce dal loop e dalla funzione
        except Exception as e:
            log.warning(f"Dashboard: Attesa per Cassandra... ({e})")
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
        # Questo non dovrebbe più accadere, ma è una sicurezza
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
    """Placeholder per i dati storici (Batch Layer)."""
    dummy_data = {
        "A1": {"avg_temp_last_month": 20.5},
        "B1": {"avg_temp_last_month": 22.1},
        "C1": {"avg_temp_last_month": 19.8}
    }
    return jsonify(dummy_data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)