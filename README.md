# DataFibers Smart GW
[![Gitter](https://badges.gitter.im/datafibers/df.svg)](https://gitter.im/datafibers/df?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![Build Status](https://travis-ci.org/datafibers/df.svg?branch=master)](https://travis-ci.org/datafibers/df)
##1.Overview
DataFibers - DF is a open source big data smart gateway and data bus for enterprise big data project. It has implemented a generic architecture for both batch and real time processing.

This project is using or will use following technologies.

* Vertx (Java 8)
* Kafka (API, Connect, Stream)
* HDFS API
* Flink|Spark


It is a maven multi-module project. It contains following modules

* **df-reactive-client**: Reads a very large file and streams it to server
* **df-reactive-server**: Non-blocking server, that reads stream of data from client, parses data and sends it to Kafka queue.

##2.TODO
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
