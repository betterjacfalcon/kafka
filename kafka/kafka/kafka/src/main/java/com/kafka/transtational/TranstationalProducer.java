package com.kafka.transtational;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;

public class TranstationalProducer{
	public static final org.slf4j.Logger log = LoggerFactory.getLogger("TranstationalProducer");
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
		props.put("acks","all");
		props.put("transactional.id","devs4j-producer-id");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms","10");
		
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
			try  {
					producer.initTransactions();
					producer.beginTransaction();				
					for(int i= 0; i< 1000000;i++) {
					producer.send(new
					ProducerRecord<String, String>("devs4j-topic",String.valueOf(i),"devs4j-message"));
				
					 //if (i==50000) { throw new Exception ("Error"); }
					 
					}
					producer.commitTransaction();
					producer.flush();
				} catch(Exception e) { 
					log.error("Error", e);
					producer.abortTransaction();
				}
			} 
			log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));
	}

	}


