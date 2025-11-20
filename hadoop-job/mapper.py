#!/usr/bin/env python3
"""
mapper.py - Versione Semplificata
Accetta TUTTI i dati in input senza filtri temporali.
"""
import sys
import json
from datetime import datetime

def main():
    for line in sys.stdin:
        try:
            line = line.strip()
            if not line: continue
            
            data = json.loads(line)
            
            # Estrazione dati base
            sensor_id = data.get("sensor_id")
            temp = data.get("temp")
            timestamp_str = data.get("timestamp")

            if sensor_id and temp is not None and timestamp_str:
                # Parsing minimale per estrarre la data
                # Supporta sia con che senza microsecondi
                if '.' in timestamp_str:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                
                # UNIX Timestamp per ordinamento
                timestamp_unix = int(dt.timestamp())
                
                # Data per la chiave (YYYY-MM-DD)
                date_str = dt.strftime('%Y-%m-%d')
                
                # Chiave composta per il partizionamento
                output_key = "{}-{}".format(sensor_id, date_str)
                
                # Valore: Temp + Timestamp
                output_value = "{}|{}".format(float(temp), timestamp_unix)
                
                print("{}\t{}".format(output_key, output_value))

        except Exception:
            # Ignora righe malformate
            pass

if __name__ == "__main__":
    main()