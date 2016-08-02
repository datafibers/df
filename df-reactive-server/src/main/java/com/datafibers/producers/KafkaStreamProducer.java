package com.datafibers.producers;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This class is used to write/stream data, such as metadata, to kafka.
 * One example of usage is to write metadata to kafka for real-time data discovery and management.
 */

public class KafkaStreamProducer {
	
	Producer<String,String> producer = null;

	public KafkaStreamProducer() {
	
		Properties properties = new Properties();
	    properties.put("metadata.broker.list","127.0.0.1:9092");
	    properties.put("serializer.class","kafka.serializer.StringEncoder");
	    ProducerConfig producerConfig = new ProducerConfig(properties);
	    
	    producer = new Producer<String, String>(producerConfig);
	    
	}
	
	public void sendMessages(String topic, String message) throws Exception
	{
		KeyedMessage<String, String> kmessage =new KeyedMessage<String, String>(topic, message);
		// producer.send(kmessage); // TODO: to uncomment after
	}
	
	public void closeProducer(){
		producer.close();
	}

}
