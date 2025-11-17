import os
import json
import logging
import docker
import subprocess
import tempfile
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from hdfs import InsecureClient
from collections import defaultdict

# --- Configurazione ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

# --- Variabili di Connessione ---
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra-seed')
CASSANDRA_KEYSPACE = os.environ.get('CASSANDRA_KEYSPACE', 'iot_keyspace')
HDFS_HOST = os.environ.get('HDFS_HOST', 'namenode')
HDFS_PORT = os.environ.get('HDFS_PORT', 9870)
HDFS_USER = os.environ.get('HDFS_USER', 'root')
HDFS_OUTPUT_DIR = '/iot-output/daily-averages'
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'

# --- INIZIO MODIFICA: Percorso per le statistiche aggregate micro-batch ---
HDFS_STATS_DIR = '/iot-stats/daily-aggregate'
# --- FINE MODIFICA ---

# --- Connessioni Globali ---
try:
    hdfs_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
    log.info("Connesso a HDFS.")
except Exception as e:
    log.error(f"Impossibile connettersi a HDFS: {e}")
    hdfs_client = None

try:
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
    )
    cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
    log.info("Connesso a Cassandra.")
except Exception as e:
    log.error(f"Impossibile connettersi a Cassandra: {e}")
    cassandra_session = None

try:
    docker_client = docker.from_env()
    log.info("Connesso a Docker socket.")
except Exception as e:
    log.error(f"Impossibile connettersi a Docker socket: {e}")
    docker_client = None

# --- 1. Serving della Dashboard (Pagina Principale) ---

@app.route('/')
def index():
    return render_template('index.html')

# --- 2. API per lo SPEED LAYER (Cassandra) ---
# (Questa sezione è invariata)

@app.route('/data/realtime')
def get_realtime_data():
    sensor_id = request.args.get('sensor_id')
    if not sensor_id: return jsonify({"error": "sensor_id mancante"}), 400
    if not cassandra_session: return jsonify({"error": "Cassandra non connesso"}), 500
    sensor_data = {}
    try:
        query = SimpleStatement(f"SELECT temp, timestamp FROM sensor_data WHERE sensor_id = %s LIMIT 1")
        row = cassandra_session.execute(query, (sensor_id,)).one()
        if row: sensor_data = {"temp": row.temp, "status": "ONLINE"}
        else: sensor_data = {"temp": "N/A", "status": "NO_DATA"}
    except Exception as e:
        log.error(f"Errore query Cassandra per {sensor_id}: {e}")
        sensor_data = {"temp": "N/A", "status": "ERROR"}
    return jsonify(sensor_data)

@app.route('/data/realtime/trend')
def get_realtime_trend():
    sensor_id = request.args.get('sensor_id')
    if not sensor_id: return jsonify({"error": "sensor_id mancante"}), 400
    if not cassandra_session: return jsonify({"error": "Cassandra non connesso"}), 500
    try:
        query_first = SimpleStatement("SELECT timestamp FROM sensor_data WHERE sensor_id = %s ORDER BY timestamp ASC LIMIT 1")
        first_row = cassandra_session.execute(query_first, (sensor_id,)).one()
        if not first_row:
            start_time = datetime.utcnow().replace(second=0, microsecond=0)
            end_time = start_time + timedelta(hours=12)
        else:
            start_time = first_row.timestamp.replace(second=0, microsecond=0)
            end_time = start_time + timedelta(hours=12)
    except Exception as e:
        log.error(f"Errore nel trovare il primo timestamp per {sensor_id}: {e}")
        start_time = datetime.utcnow().replace(second=0, microsecond=0)
        end_time = start_time + timedelta(hours=12)
    
    aggregated_data = defaultdict(list)
    data_points = []
    try:
        query = SimpleStatement("SELECT timestamp, temp FROM sensor_data WHERE sensor_id = %s AND timestamp >= %s AND timestamp <= %s")
        rows = cassandra_session.execute(query, (sensor_id, start_time, end_time))
        for row in rows:
            truncated_ts = row.timestamp.replace(second=0, microsecond=0)
            aggregated_data[truncated_ts].append(row.temp)
        for ts, temps_list in aggregated_data.items():
            if temps_list:
                avg_temp = sum(temps_list) / len(temps_list)
                data_points.append({"x": ts.isoformat(), "y": round(avg_temp, 2)})
        return jsonify({
            "data": sorted(data_points, key=lambda k: k['x']),
            "time_min": start_time.isoformat(),
            "time_max": end_time.isoformat()
        })
    except Exception as e:
        log.error(f"Errore query trend Cassandra per {sensor_id}: {e}")
        return jsonify({"data": [], "time_min": start_time.isoformat(), "time_max": end_time.isoformat()})


# --- 3. API per il BATCH LAYER (HDFS) ---

# --- INIZIO MODIFICA: Logica di aggregazione Micro-Batch ---
@app.route('/data/batch')
def get_batch_data():
    sensor_id = request.args.get('sensor_id')
    if not sensor_id: return jsonify({"error": "sensor_id mancante"}), 400
    if not hdfs_client: return jsonify({"error": "HDFS non connesso"}), 500
    
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    date_dir_path = f"{HDFS_OUTPUT_DIR}/date={today_str}"
    
    all_micro_batches = []
    
    try:
        # 1. Controlla se la directory della data esiste
        if not hdfs_client.status(date_dir_path, strict=False):
            return jsonify({"status": "Directory HDFS non trovata. Attendi il prossimo job."})
        
        # 2. Lista tutte le directory dei micro-batch (es. time=18-30-00)
        time_dirs = hdfs_client.list(date_dir_path)
        if not time_dirs:
            return jsonify({"status": "Nessun micro-batch trovato per oggi. Attendi."})
        
        # 3. Leggi il file part-00000 da OGNI micro-batch
        for time_dir in sorted(time_dirs): # Ordina per ora
            part_file_path = f"{date_dir_path}/{time_dir}/part-00000"
            if hdfs_client.status(part_file_path, strict=False):
                with hdfs_client.read(part_file_path, encoding='utf-8') as reader:
                    for line in reader:
                        line = line.strip()
                        if not line: continue
                        try:
                            key, json_str = line.split('\t', 1)
                            # Salva solo i dati del sensore richiesto
                            if key == f"{sensor_id}-{today_str}":
                                metrics = json.loads(json_str)
                                all_micro_batches.append(metrics)
                        except (ValueError, json.JSONDecodeError) as e:
                            log.warning(f"Errore parsing riga batch: {line[:100]}... - {e}")
                            continue
                            
        if not all_micro_batches:
            return jsonify({"status": f"Nessun dato batch trovato per {sensor_id}. Attendi."})
        
        # 4. Aggrega i risultati dei micro-batch in una singola metrica "Daily"
        final_metrics = {
            "open": all_micro_batches[0]['open'], # Open del primo batch
            "close": all_micro_batches[-1]['close'], # Close dell'ultimo batch
            "min": min(b['min'] for b in all_micro_batches),
            "max": max(b['max'] for b in all_micro_batches),
            "count": sum(b['count'] for b in all_micro_batches),
            "total_count": sum(b['total_count'] for b in all_micro_batches),
            "discarded_count": sum(b['discarded_count'] for b in all_micro_batches),
        }
        
        # Ricalcola le percentuali basate sui dati aggregati
        open_price = final_metrics['open']
        close_price = final_metrics['close']
        
        daily_change = close_price - open_price
        daily_change_pct = (daily_change / open_price) * 100 if open_price > 0 else 0
        
        price_range = final_metrics['max'] - final_metrics['min']
        range_pct = (price_range / open_price) * 100 if open_price > 0 else 0
        
        final_metrics["daily_change"] = round(daily_change, 2)
        final_metrics["daily_change_pct"] = round(daily_change_pct, 2)
        final_metrics["range_pct"] = round(range_pct, 2)
        
        # La volatilità è troppo complessa da ricalcolare, usiamo la media
        avg_volatility = sum(b['volatility'] for b in all_micro_batches) / len(all_micro_batches)
        final_metrics["volatility"] = round(avg_volatility, 2)

        # La dashboard si aspetta un dizionario {data: metriche}
        return jsonify({today_str: final_metrics})

    except Exception as e:
        log.error(f"Errore durante la lettura dei dati batch da HDFS: {e}")
        return jsonify({"error": f"Errore HDFS: {e}"}), 500
# --- FINE MODIFICA ---


# --- 4. API per l'Avvio del Job (Docker) ---
# (Invariato)
@app.route('/trigger-job', methods=['POST'])
def trigger_mapreduce_job():
    if not docker_client: return jsonify({"error": "Docker client non inizializzato"}), 500
    try:
        container_name = 'nodemanager'
        container = docker_client.containers.get(container_name)
        cmd_to_run = "/app/run_job.sh" 
        log.info(f"Esecuzione di '{cmd_to_run}' su container '{container_name}'...")
        container.exec_run(cmd=cmd_to_run, detach=True) 
        return jsonify({"status": "Job avviato con successo."})
    except docker.errors.NotFound:
        log.error(f"Container '{container_name}' non trovato.")
        return jsonify({"error": f"Container '{container_name}' non trovato."}), 404
    except Exception as e:
        log.error(f"Errore nell'avvio del job: {e}")
        return jsonify({"error": str(e)}), 500

# --- 5. API per le PRESTAZIONI (Docker Stats) ---
# (Invariato)
@app.route('/data/performance')
def get_system_performance():
    if not docker_client: 
        log.error("API Performance: Docker client non connesso.")
        return jsonify({"error": "Docker client non connesso"}), 500
    
    containers_to_monitor = [
        'iot-producer', 'dashboard', 'namenode', 'datanode', 
        'resourcemanager', 'nodemanager', 'cassandra-seed', 
        'cassandra-node1', 'init-services'
    ]
    stats_data = {}
    
    try:
        for container_name in containers_to_monitor:
            try:
                container = docker_client.containers.get(container_name)
                stats = container.stats(stream=False) 
                
                mem_usage = stats['memory_stats'].get('usage', 0) / (1024 * 1024)
                
                net_rx = 0
                net_tx = 0
                if 'networks' in stats and stats['networks'] is not None:
                    for if_name, data in stats['networks'].items():
                        net_rx += data.get('rx_bytes', 0)
                        net_tx += data.get('tx_bytes', 0)
                
                disk_write = 0
                if 'blkio_stats' in stats and \
                   'io_service_bytes_recursive' in stats['blkio_stats'] and \
                   stats['blkio_stats']['io_service_bytes_recursive'] is not None:
                    for item in stats['blkio_stats']['io_service_bytes_recursive']:
                        if item['op'] == 'Write': disk_write += item['value']
                
                stats_data[container_name] = {
                    "mem_mb": round(mem_usage, 2),
                    "net_rx_mb": round(net_rx / (1024 * 1024), 2),
                    "net_tx_mb": round(net_tx / (1024 * 1024), 2),
                    "disk_write_mb": round(disk_write / (1024 * 1024), 2)
                }
                
            except docker.errors.NotFound:
                log.warning(f"API Performance: Container '{container_name}' non trovato.")
                stats_data[container_name] = {"mem_mb": 0, "net_rx_mb": 0, "net_tx_mb": 0, "disk_write_mb": 0} 
            except KeyError:
                log.warning(f"API Performance: Dati incompleti per '{container_name}'.")
                stats_data[container_name] = {"mem_mb": 0, "net_rx_mb": 0, "net_tx_mb": 0, "disk_write_mb": 0}
    
        return jsonify(stats_data)
    
    except Exception as e:
        log.error(f"Errore grave durante il recupero delle statistiche Docker: {e}")
        return jsonify({"error": str(e)}), 500

# --- 6. API per STATISTICHE AGGREGATE (Dati Puliti + Scartati) ---

# --- INIZIO MODIFICA: Aggregazione Micro-Batch per le statistiche ---
@app.route('/data/aggregate_stats')
def get_aggregate_stats():
    if not hdfs_client:
        return jsonify({"error": "HDFS non connesso"}), 500
    
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    date_dir_path = f"{HDFS_STATS_DIR}/date={today_str}"
    
    total_clean = 0
    total_discarded = 0
    total_processed = 0
    
    try:
        # 1. Controlla se la directory della data esiste
        if not hdfs_client.status(date_dir_path, strict=False):
            log.warning(f"Directory statistiche non trovata: {date_dir_path}")
            return jsonify({"total_clean": 0, "total_discarded": 0, "total_processed": 0})
        
        # 2. Lista tutte le directory dei micro-batch (es. time=18-30-00)
        time_dirs = hdfs_client.list(date_dir_path)
        if not time_dirs:
            return jsonify({"total_clean": 0, "total_discarded": 0, "total_processed": 0})

        # 3. Leggi il file aggregate_stats.json da OGNI micro-batch
        for time_dir in time_dirs:
            stats_file_path = f"{date_dir_path}/{time_dir}/aggregate_stats.json"
            if hdfs_client.status(stats_file_path, strict=False):
                try:
                    with hdfs_client.read(stats_file_path, encoding='utf-8') as reader:
                        stats = json.load(reader)
                        total_clean += stats.get("total_clean", 0)
                        total_discarded += stats.get("total_discarded", 0)
                        total_processed += stats.get("total_processed", 0)
                except Exception as e:
                    log.warning(f"Impossibile leggere il file di statistiche {stats_file_path}: {e}")

        return jsonify({
            "total_clean": total_clean,
            "total_discarded": total_discarded,
            "total_processed": total_processed
        })
    
    except Exception as e:
        log.error(f"Errore durante il calcolo aggregate_stats: {e}")
        return jsonify({"total_clean": 0, "total_discarded": 0, "total_processed": 0})
# --- FINE MODIFICA ---


# --- 7. API per STATISTICHE SCARTI (Speed Layer) ---
# (Invariato)
@app.route('/data/discard_stats')
def get_discard_stats():
    if not hdfs_client: 
        return jsonify({"error": "HDFS non connesso"}), 500
    
    default_stats = {"previous": 0, "current": 0, "total": 0}
    
    try:
        if not hdfs_client.status(HDFS_DISCARD_STATS_PATH, strict=False):
            log.warning(f"{HDFS_DISCARD_STATS_PATH} non ancora creato.")
            return jsonify(default_stats)

        with hdfs_client.read(HDFS_DISCARD_STATS_PATH, encoding='utf-8') as reader:
            stats = json.load(reader)
        
        try:
            previous = int(stats.get("previous") or 0)
            current = int(stats.get("current") or 0)
            stats["previous"] = previous
            stats["current"] = current
            stats["total"] = previous + current
        except (ValueError, TypeError):
            stats["total"] = 0
            log.warning("Valori non validi in discard_stats.json")
        
        return jsonify(stats)
    
    except Exception as e:
        log.error(f"Errore durante la lettura di {HDFS_DISCARD_STATS_PATH}: {e}")
        return jsonify(default_stats)


# --- Avvio del Server ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)