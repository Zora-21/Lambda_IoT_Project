# Partiamo dall'immagine originale del nodemanager
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# Passiamo a root per modificare i file e installare
USER root

# CORREZIONE: Poiché l'immagine è basata su Debian "Stretch" (che è EOL),
# dobbiamo puntare ai repository di archivio prima di eseguire 'apt-get update'.
RUN echo "deb http://archive.debian.org/debian/ stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security/ stretch/updates main" >> /etc/apt/sources.list

# Ora possiamo aggiornare e installare python3
RUN apt-get update && apt-get install -y python3 python3-pip