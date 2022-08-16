package com.kafka.producers;

import java.lang.System.Logger;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;

public class PKafkaProducer {
	
	public static final org.slf4j.Logger log = LoggerFactory.getLogger("PKafkaProducer");
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
		props.put("acks","1");
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
				for(int i= 0; i< 1000000;i++) {
				producer.send(new
				ProducerRecord<String, String>("devs4j-topic",String.valueOf(i),"devs4j-message"));
				}
				producer.flush();
			} 
		log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));
	}

}
