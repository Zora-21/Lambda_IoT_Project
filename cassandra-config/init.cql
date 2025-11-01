/*/Users/matteo/Documents/Lambda_IoT/cassandra-config/init.cql*/
/* Crea il Keyspace 'moviedb' se non esiste.
 * Usiamo 'SimpleStrategy' con replication_factor: 2 
 * perché nel docker-compose hai due nodi (seed e node1).
 */
CREATE KEYSPACE IF NOT EXISTS moviedb
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 };

/* Usa il keyspace appena creato */
USE moviedb;

/* Crea la tabella 'sensor_data' come da specifiche del PDF [cite: 63-68] */
CREATE TABLE IF NOT EXISTS sensor_data (
  sensor_id TEXT,
  timestamp TIMESTAMP,
  temp FLOAT,
  PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);