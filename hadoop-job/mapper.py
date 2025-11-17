#!/usr/bin/env python3
"""
mapper.py - Script Map avanzato per analisi finanziarie.

Legge i log JSON dei sensori da stdin, estrae il sensor_id,
la data (dal timestamp), la temperatura e il timestamp completo.

ACCETTA --filter-minutes per processare solo i dati recenti.

Emette una coppia chiave-valore separata da tab:
CHIAVE: <sensor_id>-<data>
VALORE: <temperatura>|<timestamp_unix>
"""

import sys
import json
import argparse
from datetime import datetime, timedelta, timezone

# --- Logica "Micro-Batch": Argomento per il filtro temporale ---
parser = argparse.ArgumentParser()
parser.add_argument(
    "--filter-minutes",
    type=int,
    default=0, # 0 significa nessun filtro
    help="Filtra i dati di input per gli ultimi N minuti."
)
# Hadoop Streaming passa gli argomenti dopo --
# Dobbiamo parsare solo quelli che conosciamo
args, unknown = parser.parse_known_args()

# --- INIZIO CORREZIONE BUG ---
# L'attributo deve usare l'underscore (filter_minutes), non il trattino (filter-minutes)
filter_active = args.filter_minutes > 0
# --- FINE CORREZIONE BUG ---

cutoff_time = None
if filter_active:
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=args.filter_minutes)
    # Stampa un log su stderr per il debug
    print("Mapper Filtro Attivo: processa solo dati dopo le {}".format(cutoff_time), file=sys.stderr)
# --- Fine ---


for line in sys.stdin:
    try:
        # Rimuove spazi bianchi e carica il JSON
        line = line.strip()
        data = json.loads(line)

        # Estrae i dati necessari
        sensor_id = data.get("sensor_id")
        temp = data.get("temp")
        timestamp_str = data.get("timestamp")  # Formato ISO: 2025-10-31T10:00:00.123

        # Assicurati che tutti i dati siano presenti
        if sensor_id and temp is not None and timestamp_str:
            
            # --- Logica "Micro-Batch": Filtro sul timestamp ---
            try:
                # fromisoformat() non disponibile in Python 3.5, usare strptime()
                # Supporta formato: 2025-11-16T20:35:29.514000 e 2025-11-16T20:35:29Z
                ts_clean = timestamp_str.replace('Z', '')
                if '.' in ts_clean:
                    # Con microseconds
                    dt = datetime.strptime(ts_clean, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    # Senza microseconds
                    dt = datetime.strptime(ts_clean, "%Y-%m-%dT%H:%M:%S")
                
                dt = dt.replace(tzinfo=timezone.utc)
                
                if filter_active and dt < cutoff_time:
                    continue # Dato troppo vecchio, scarta
                
                timestamp_unix = int(dt.timestamp())
            
            except Exception as e_ts:
                print("Errore parsing timestamp mapper: {}".format(e_ts), file=sys.stderr)
                timestamp_unix = 0
            # --- Fine ---

            # Estrae solo la parte della data (YYYY-MM-DD)
            date_str = timestamp_str.split('T')[0]
            
            # Crea la chiave composita
            output_key = "{}-{}".format(sensor_id, date_str)
            
            # Emette temp|timestamp per permettere ordinamento e calcoli avanzati
            output_value = "{}|{}".format(float(temp), timestamp_unix)
            
            # Emette la coppia chiave-valore su stdout
            print("{}\t{}".format(output_key, output_value))

    except Exception as e:
        # Ignora le righe malformate
        pass