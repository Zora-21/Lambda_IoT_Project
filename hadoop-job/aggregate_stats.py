#!/usr/bin/env python3
"""
aggregate_stats.py

Legge l'output del job MapReduce (reducer.py) da stdin.
Analizza il JSON di ogni riga per estrarre 'count' (puliti) 
e 'discarded_count' (scartati).

Emette un singolo JSON su stdout con i totali globali.
"""

import sys
import json

total_clean = 0
total_discarded = 0

for line in sys.stdin:
    try:
        # L'output del reducer è: CHIAVE \t JSON
        key, json_str = line.strip().split('\t', 1)
        metrics = json.loads(json_str)
        
        # 'count' in reducer.py è il conteggio dei dati PULITI
        total_clean += metrics.get('count', 0) 
        # 'discarded_count' è il conteggio dei dati SCARTATI
        total_discarded += metrics.get('discarded_count', 0)
        
    except (ValueError, json.JSONDecodeError):
        # Ignora righe malformate o che non sono output del reducer
        pass

# Calcola il totale complessivo
total_processed = total_clean + total_discarded

# Stampa il JSON finale su stdout
output = {
    "total_clean": total_clean,
    "total_discarded": total_discarded,
    "total_processed": total_processed
}
print(json.dumps(output, indent=2))