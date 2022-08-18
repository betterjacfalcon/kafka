package com.kafka.thread.consumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MultithreadConsumer {

	public static void main(String[] args) {
		Properties props=new Properties();
		
		props.setProperty("bootstrap.servers","localhost:9092");
		props.setProperty("group.id","devs4j-group");
		props.setProperty("enable.auto.commit","true");
		props.setProperty("auto.commit.interval.ms","1000");
		props.setProperty("key.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");		

		//Config properties
		ExecutorService executor= Executors.newFixedThreadPool(5);
		for(int i = 0;i< 5; i++) {
			ThreadConsumer consumer= new ThreadConsumer(new KafkaConsumer<>(props));
			executor.execute(consumer);
		}
			executor.shutdown();
		while(!executor.isTerminated()) ;
		}
}


