#/Users/matteo/Documents/Lambda_IoT/hadoop-job/run_job.sh
#!/bin/bash

# Definiamo l'indirizzo corretto del NameNode
HDFS_URI="hdfs://namenode:9000"

# Nome della directory di output su HDFS
OUTPUT_DIR="/iot-output/daily-averages"

# Nome della directory di input su HDFS
# Assicurati che questo corrisponda a dove il tuo producer.py sta scrivendo!
INPUT_DIR="/iot-data/*" 

echo "Avvio del job MapReduce per le medie giornaliere..."

# Rimuove la cartella di output precedente, se esiste
# CORREZIONE: Aggiunto -fs per specificare il NameNode
hdfs dfs -fs $HDFS_URI -rm -r $OUTPUT_DIR

# Esegue il job hadoop-streaming
# CORREZIONE: Aggiunto -fs per specificare il NameNode
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapred.job.name="IoT Daily Temp Avg" \
    -fs $HDFS_URI \
    -files /app/mapper.py,/app/reducer.py \
    -mapper /app/mapper.py \
    -reducer /app/reducer.py \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR

echo "Job completato. Controllo dei risultati:"

# Mostra i primi 10 risultati
# CORREZIONE: Aggiunto -fs per specificare il NameNode
hdfs dfs -fs $HDFS_URI -cat $OUTPUT_DIR/part-00000 | head