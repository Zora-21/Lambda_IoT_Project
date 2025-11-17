#!/bin/bash

# --- Imposta JAVA_HOME per cron ---
source /opt/hadoop-3.2.1/etc/hadoop/hadoop-env.sh

export HADOOP_HOME=/opt/hadoop-3.2.1
HDFS_CMD="$HADOOP_HOME/bin/hdfs"
HADOOP_CMD="$HADOOP_HOME/bin/hadoop"
HDFS_URI="hdfs://namenode:9000"

MODEL_FILE_HDFS="/models/model.json"
MODEL_EXISTS_BEFORE_RUN="no"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - run_job.sh - $1"
}

if $HDFS_CMD dfs -fs $HDFS_URI -test -e $MODEL_FILE_HDFS; then
    log "Modello trovato. Si procederà con l'analisi storica (Fase 2 e 3)."
    MODEL_EXISTS_BEFORE_RUN="yes"
else
    log "Modello non trovato. Verrà creato (Fase 1), ma le analisi storiche (Fase 2 e 3) saranno saltate per questo ciclo."
fi

# --- FASE 1: ADDESTRAMENTO MODELLO (PULIZIA) ---
TODAY_DATE=$(date +%Y-%m-%d)
YESTERDAY_DATE=$(date -d "yesterday" +%Y-%m-%d)

log "Fase 1: Addestramento modello (train_model.py) su dati recenti (ieri e oggi)..."
MODEL_FILE_PATH="/app/model.json" 

# Comando 'cat' robusto (Corretto)
{
  $HDFS_CMD dfs -fs $HDFS_URI -cat /iot-data/date=$YESTERDAY_DATE/*.jsonl 2>/dev/null
  $HDFS_CMD dfs -fs $HDFS_URI -cat /iot-data/date=$TODAY_DATE/*.jsonl 2>/dev/null
} | python3 /app/train_model.py > $MODEL_FILE_PATH

log "Modello salvato localmente in $MODEL_FILE_PATH"
cat $MODEL_FILE_PATH

log "Caricamento modello aggiornato su HDFS..."
$HDFS_CMD dfs -fs $HDFS_URI -mkdir -p /models
$HDFS_CMD dfs -fs $HDFS_URI -put -f $MODEL_FILE_PATH $MODEL_FILE_HDFS
log "Modello caricato in $MODEL_FILE_HDFS"


if [ "$MODEL_EXISTS_BEFORE_RUN" = "yes" ]; then

    # --- FASE 2: JOB MAPREDUCE (Logica Micro-Batch) ---
    INPUT_DIR="/iot-data/date=$TODAY_DATE/*"
    
    # --- INIZIO CORREZIONE LOGICA MICRO-BATCH ---
    # Crea una directory univoca per questo micro-batch
    CURRENT_TIME=$(date +%H-%M-%S)
    OUTPUT_DIR="/iot-output/daily-averages/date=$TODAY_DATE/time=$CURRENT_TIME"
    # --- FINE CORREZIONE ---

    log "Fase 2: Avvio job MapReduce (Micro-Batch) per i dati di OGGI ($TODAY_DATE)..."
    
    # Non cancelliamo più la directory padre, solo quella del job se esiste
    $HDFS_CMD dfs -fs $HDFS_URI -rm -r -skipTrash $OUTPUT_DIR 2>/dev/null || true

    MAPPER_CMD="python3 /app/mapper.py --filter-minutes 10"

    $HADOOP_CMD jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -D mapred.job.name="Crypto Financial Stats (Micro-Batch) - $TODAY_DATE $CURRENT_TIME" \
        -fs $HDFS_URI \
        -files /app/mapper.py,/app/reducer.py,$MODEL_FILE_PATH \
        -mapper "$MAPPER_CMD" \
        -reducer "python3 /app/reducer.py" \
        -input $INPUT_DIR \
        -output $OUTPUT_DIR

    log "Job completato. Controllo dei risultati (puliti) per $TODAY_DATE $CURRENT_TIME:"
    $HDFS_CMD dfs -fs $HDFS_URI -cat $OUTPUT_DIR/part-00000 | head

    # --- FASE 3: AGGREGAZIONE STATISTICHE (Logica Micro-Batch) ---
    log "Fase 3: Aggregazione statistiche finali..."
    
    # --- INIZIO CORREZIONE LOGICA MICRO-BATCH ---
    # Salva le statistiche in un percorso univoco
    STATS_OUTPUT_PATH="/iot-stats/daily-aggregate/date=$TODAY_DATE/time=$CURRENT_TIME"
    # --- FINE CORREZIONE ---
    
    $HDFS_CMD dfs -fs $HDFS_URI -mkdir -p $STATS_OUTPUT_PATH

    AGGREGATE_JSON=$($HDFS_CMD dfs -fs $HDFS_URI -cat $OUTPUT_DIR/part-00000 | python3 /app/aggregate_stats.py)
    log "Statistiche aggregate:"
    echo "$AGGREGATE_JSON"

    echo "$AGGREGATE_JSON" > /tmp/aggregate_stats.json
    $HDFS_CMD dfs -fs $HDFS_URI -put -f /tmp/aggregate_stats.json $STATS_OUTPUT_PATH/aggregate_stats.json
    log "✅ Statistiche aggregate salvate in $STATS_OUTPUT_PATH/aggregate_stats.json"

else
    log "FASE 2 e 3 saltate: Il modello è stato appena creato."
    log "Le analisi storiche inizieranno al prossimo ciclo."
fi

log "Creazione del file trigger per la rotazione dei contatori..."
touch /tmp/rotate_trigger_file
$HDFS_CMD dfs -fs $HDFS_URI -put -f /tmp/rotate_trigger_file /models/rotate_trigger
log "File trigger caricato in /models/rotate_trigger"