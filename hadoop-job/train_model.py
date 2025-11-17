#!/usr/bin/env python3
import sys
import json
import math
from datetime import datetime, timedelta
import statistics

def main():
    temps_by_sensor = {} # Dizionario per raggruppare le temperature
    temps_by_sensor_all = {} # Tutti i dati senza filtro
    
    # Calcola il timestamp di 10 minuti fa (buffer più grande)
    now = datetime.utcnow()
    time_window_ago = now - timedelta(minutes=10)
    
    sys.stderr.write("Addestramento modello: cercando dati negli ultimi 10 minuti\n")
    sys.stderr.write("Ora corrente: {}\n".format(now.isoformat()))
    sys.stderr.write("Limite temporale: {}\n".format(time_window_ago.isoformat()))

    # Flag per tracciare se abbiamo letto almeno una riga
    lines_read = 0

    # Legge tutti i dati da stdin (provenienti da HDFS)
    for line in sys.stdin:
        lines_read += 1
        try:
            data = json.loads(line.strip())
            sensor_id = data.get("sensor_id")
            temp = data.get("temp")
            timestamp_str = data.get("timestamp")

            if sensor_id and temp is not None and timestamp_str:
                # Converte il timestamp ISO string a datetime
                try:
                    # Prova il formato standard ISO con microsecond i
                    data_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')
                except:
                    # Fallback per altri formati
                    try:
                        data_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                    except:
                        sys.stderr.write("Impossibile parsare timestamp: {}\n".format(timestamp_str))
                        continue
                
                # Salva TUTTI i dati (fallback)
                if sensor_id not in temps_by_sensor_all:
                    temps_by_sensor_all[sensor_id] = []
                temps_by_sensor_all[sensor_id].append(float(temp))
                
                # FILTRO: Considera solo i dati degli ultimi 10 minuti
                if data_timestamp >= time_window_ago:
                    if sensor_id not in temps_by_sensor:
                        temps_by_sensor[sensor_id] = []
                    temps_by_sensor[sensor_id].append(float(temp))

        except Exception as e:
            sys.stderr.write("Errore durante il parsing: {} - Riga: {}\n".format(e, line))
            pass # Ignora righe malformate

    sys.stderr.write("Totale righe elaborate: {}\n".format(lines_read))
    
    # Se non abbiamo dati, ritorna un modello vuoto (il producer aspetterà)
    if not temps_by_sensor_all:
        sys.stderr.write("⚠️  ATTENZIONE: Nessun dato trovato in HDFS. Nessun modello creato ancora.\n")
        print(json.dumps({}))
        return

    model = {}

    # Decidi quali dati usare: preferisci gli ultimi 5 minuti, ma fallback a TUTTI se necessario
    temps_to_use = temps_by_sensor.copy() if temps_by_sensor else {}
    
    # Se non hai abbastanza dati recenti, usa TUTTI i dati
    for sensor_id in temps_by_sensor_all:
        if sensor_id not in temps_to_use or len(temps_to_use.get(sensor_id, [])) < 5:
            sys.stderr.write("FALLBACK: Sensore {} non ha dati recenti sufficienti, usando TUTTI i dati\n".format(sensor_id))
            temps_to_use[sensor_id] = temps_by_sensor_all[sensor_id]

    # Calcola media e deviazione standard per ogni sensore
    for sensor_id, temps in temps_to_use.items():
        n = len(temps)
        sys.stderr.write("Sensore {}: {} dati validi\n".format(sensor_id, n))
        
        if n > 4: # Richiede almeno 5 punti dati per un filtro IQR sensato

            # --- INIZIO FILTRAGGIO RUMORE (IQR) ---

            # 1. Ordina i dati
            temps.sort()

            # Percentile con interpolazione lineare
            def percentile(sorted_arr, p):
                if not sorted_arr:
                    return None
                k = (len(sorted_arr) - 1) * p
                f = math.floor(k)
                c = math.ceil(k)
                if f == c:
                    return sorted_arr[int(k)]
                d0 = sorted_arr[int(f)] * (c - k)
                d1 = sorted_arr[int(c)] * (k - f)
                return d0 + d1

            # 2. Calcola Q1 (25°) e Q3 (75°)
            q1 = percentile(temps, 0.25)
            q3 = percentile(temps, 0.75)

            iqr = (q3 - q1) if (q1 is not None and q3 is not None) else 0

            # 3. Calcola i limiti per il "rumore" (1.5 * IQR)
            lower_bound = q1 - (1.5 * iqr) if q1 is not None else None
            upper_bound = q3 + (1.5 * iqr) if q3 is not None else None

            # 4. Filtra i dati usando i limiti calcolati
            if lower_bound is None or upper_bound is None:
                cleaned_temps = temps[:]
            else:
                cleaned_temps = [t for t in temps if lower_bound <= t <= upper_bound]

            # Se il filtro IQR rimuove troppi punti, applica un fallback meno aggressivo
            if len(cleaned_temps) < max(2, int(0.3 * n)):
                lower_fb = percentile(temps, 0.05)
                upper_fb = percentile(temps, 0.95)
                if lower_fb is not None and upper_fb is not None and lower_fb < upper_fb:
                    cleaned_temps_fb = [t for t in temps if lower_fb <= t <= upper_fb]
                    if len(cleaned_temps_fb) >= 2:
                        cleaned_temps = cleaned_temps_fb
                        sys.stderr.write("  -> IQR overly aggressive, fallback 5%-95% used ({} samples)\n".format(len(cleaned_temps_fb)))
                    else:
                        cleaned_temps = temps[:]
                        sys.stderr.write("  -> Fallback to ALL data ({} samples)\n".format(len(cleaned_temps)))

            # --- FINE FILTRAGGIO RUMORE ---

            # 5. Calcola le statistiche SUI DATI PULITI
            n_cleaned = len(cleaned_temps)

            if n_cleaned >= 1:
                mean_val = statistics.mean(cleaned_temps)
                if n_cleaned > 1:
                    std_dev_val = statistics.stdev(cleaned_temps)
                else:
                    std_dev_val = 0.0

                # Evita valori numerici troppo piccoli per std_dev che possano rompere
                # filtri downstream. Manteniamo precisione maggiore per std_dev.
                if std_dev_val < 1e-6:
                    std_dev_val = 0.0

                model[sensor_id] = {
                    "mean": round(mean_val, 2),
                    "std_dev": round(std_dev_val, 4)
                }
                sys.stderr.write("  -> Modello creato: mean={}, std_dev={} (n={})\n".format(model[sensor_id]['mean'], model[sensor_id]['std_dev'], n_cleaned))
            else:
                sys.stderr.write("  -> Insufficient cleaned data ({} samples) after IQR filtering\n".format(n_cleaned))
        else:
            sys.stderr.write("  -> Insufficient raw data ({} samples, necessari almeno 5)\n".format(n))

    # Stampa il modello (come JSON) su stdout
    print(json.dumps(model, indent=2))

if __name__ == "__main__":
    main()