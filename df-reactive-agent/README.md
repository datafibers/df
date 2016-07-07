# DF Vertx Reactive Agent 

##Overview
This is a non-blocking agent, that submit stream/batch of data to reactive sever. 

The core functions are as follows.
* Prepare HTTP header information for meta data and data for processing

##Sample Usage
java -jar df-reactive-agent-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
/home/vagrant/data/a.json \
appA \
STREAM_KAFKA 