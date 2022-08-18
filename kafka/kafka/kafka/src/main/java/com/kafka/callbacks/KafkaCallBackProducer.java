package com.kafka.callbacks;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

public class KafkaCallBackProducer {

public static final org.slf4j.Logger log = LoggerFactory.getLogger("KafkaCallBackProducer");
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
		props.put("acks","1");
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms","10");
		
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
				for(int i= 0; i< 10000;i++) {
				producer.send(new
				ProducerRecord<String, String>("devs4j-topic",String.valueOf(i),"devs4j-message"), new Callback() 
				{

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							log.info(null);
						}else {
							log.info("Offset = {}, Particion = {}, Topic = {} ", metadata.offset(), metadata.partition(), metadata.topic());
						}
						
					}
					
					
					
				});
				}
				producer.flush();
			} 
		log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

	}

}
