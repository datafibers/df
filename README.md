# DataFibers Smart GW

###1.Overview
DataFibers - DF is a Kappa Architecture implementation based on a immutable log appender model.

This project is using following technologies.

* Vertx (Java 8)
* Kafka (API, Connect, Stream)
* HDFS API
* Flink


It is a maven multi-module project. It contains following modules

* **vertx-reactive-client**: Reads a very large file and streams it to server
* **vertx-reactive-server**: Non-blocking server, that reads stream of data from client, parses data and sends it to Kafka queue.
* **flink-kafka-processor**: Uses streaming data processing framework Apache Flink to read data from Kafka and in this project it uses Cassandra DB to store the processed data.

###2.TODO
* Streaming files to Kafka - DONE
* Streaming metadata to Kafka - DONE
* Streaming files to HDFS - DONE
* Batching files to HDFS
* Batching files to HIVE
* Metadata Store
* File watcher
* Dashboard for metadata
* Transformation framework
* Persist framework
* Query framework
* Integrate Kanaba and Elastic
* File ingestion and conversion, flat, xml, csv, mainframe
* File header and trailer validation
* Data replication across clusters, databases, tables, etc
* Data policy supports, such as purging/retaining some rows for compliance reasons
* Automatically register data with Hive
* Data format interchange
* Data deduplication and merge
* Data job management and monitoring
* Web UI
