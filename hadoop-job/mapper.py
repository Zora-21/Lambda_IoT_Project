#/Users/matteo/Documents/Lambda_IoT/hadoop-job/mapper.py

#!/usr/bin/env python3
"""
mapper.py - Script Map per il job Hadoop Streaming.

Legge i log JSON dei sensori da stdin, estrae il sensor_id,
la data (dal timestamp) e la temperatura.

Emette una coppia chiave-valore separata da tab:
CHIAVE: <sensor_id>-<data>
VALORE: <temperatura>
"""

import sys
import json

for line in sys.stdin:
    try:
        # Rimuove spazi bianchi e carica il JSON
        line = line.strip()
        data = json.loads(line)

        # Estrae i dati necessari
        sensor_id = data.get("sensor_id")
        temp = data.get("temp")
        timestamp_str = data.get("timestamp") # Formato ISO: 2025-10-31T10:00:00.123

        # Assicurati che tutti i dati siano presenti
        if sensor_id and temp is not None and timestamp_str:
            # Estrae solo la parte della data (YYYY-MM-DD)
            date_str = timestamp_str.split('T')[0]
            
            # Crea la chiave composita
            output_key = f"{sensor_id}-{date_str}"
            output_value = float(temp)
            
            # Emette la coppia chiave-valore su stdout
            # Hadoop userà questa tabulazione ('\t') come separatore
            print(f"{output_key}\t{output_value}")

    except Exception as e:
        # Ignora le righe malformate (JSON non validi, ecc.)
        # È una buona pratica in MapReduce non far fallire il job per una riga
        pass