# DF Vertx Reactive Server 

##Overview
This is a non-blocking server, that reads stream/batch of data from client, parses data and sends it to Kafka queue. The core functions are as follows.

* Decode HTTP header information for meta data and type of processing
* Keep metadata in meta database as well as KAFKA

The following HTTP headers are used as meta data

* DF_PROTOCOL: [REGISTER|ACTIONS|UNREGISTER]
* DF_MODE: [STREAM_KAFKA|STREAM_HDFS|BATCH_HDFS|BATCH_HIVE]
* DF_TYPE: [META|PAYLOAD]
* DF_TOPIC: [META_DATA|Parameters]
* DF_FILTER: [NONE|JSON]

The overall data processing workflow is as follows

1. Client register and handshake
1. Client send data to server
1. Server complete process and send feedback

The overall unregister client workflow is as follows

1. Client unregister and handshake
1. Server unregister client and send feedback


##Sample Usage
java -jar df-reactive-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar