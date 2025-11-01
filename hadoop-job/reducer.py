#/Users/matteo/Documents/Lambda_IoT/hadoop-job/reducer.py

#!/usr/bin/env python3
"""
reducer.py - Script Reduce per il job Hadoop Streaming.

Riceve le coppie chiave-valore ordinate dal mapper.
Le chiavi sono <sensor_id>-<data>, i valori sono le temperature.

Calcola la somma e il conteggio per ogni chiave,
poi emette la media finale.
"""

import sys

current_key = None
current_sum = 0
current_count = 0

for line in sys.stdin:
    try:
        # Rimuove spazi bianchi e divide per la tabulazione
        line = line.strip()
        key, value = line.split('\t', 1)
        
        # Converte il valore in un numero
        temp = float(value)

        # Logica di raggruppamento
        # Se siamo ancora sulla stessa chiave, accumula i valori
        if current_key == key:
            current_sum += temp
            current_count += 1
        else:
            # Se è una nuova chiave (e non è la prima riga),
            # stampa il risultato della chiave precedente
            if current_key:
                average = current_sum / current_count
                # Emette il risultato finale: CHIAVE \t VALORE_MEDIO
                print(f"{current_key}\t{average:.2f}") # Arrotonda a 2 decimali

            # Resetta i contatori per la nuova chiave
            current_key = key
            current_sum = temp
            current_count = 1

    except ValueError:
        # Ignora righe con valori non numerici
        pass

# Non dimenticare di stampare l'ultimo gruppo!
# Il loop finisce, ma l'ultimo risultato è ancora in memoria.
if current_key:
    average = current_sum / current_count
    print(f"{current_key}\t{average:.2f}")