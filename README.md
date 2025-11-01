lambda-iot-project/
│
├── .gitignore             # File per escludere file (es. __pycache__, .env)
├── .env                   # File per le variabili d'ambiente (porte, password, ecc.)
├── docker-compose.yml     # Il file principale per orchestrare tutti i servizi
├── README.md              # Istruzioni su come avviare e usare il progetto
│
├── dashboard/             # Servizio "Serving Layer"
│   ├── Dockerfile         # Istruzioni per costruire l'immagine del dashboard
│   ├── app.py             # Applicazione web Python/Flask
│   ├── requirements.txt   # Dipendenze (es. flask, cassandra-driver)
│   └── templates/
│       └── index.html     # Pagina HTML per la dashboard
│
├── hadoop-job/            # Codice per il Batch Layer (MapReduce)
│   ├── mapper.py          # Funzione Map
│   ├── reducer.py         # Funzione Reduce
│   └── run_job.sh         # Script (opzionale) per avviare il job su YARN
│
├── iot-producer/          # Servizio "IoT Producer"
│   ├── Dockerfile         # Istruzioni per costruire l'immagine del producer
│   ├── producer.py        # Script che genera dati e li invia a HDFS e Cassandra
│   └── requirements.txt   # Dipendenze (es. cassandra-driver, hdfscli)
│
└── cassandra-config/      # Configurazione per lo "Speed Layer"
    └── init.cql           # Script per creare la tabella 'sensor_data'