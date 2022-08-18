package com.kafka.thread.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.LoggerFactory;

public class ThreadConsumer extends Thread {
	public static final org.slf4j.Logger log = LoggerFactory.getLogger("ThreadConsumer");

	private final KafkaConsumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public ThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run() {
		try{
			consumer.subscribe(Arrays.asList("devs4j-topic"));
			while(!closed.get()) {
				ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, String> consumerRecord:consumerRecords) {
					log.debug("Offset = {}, Particion = {}, Key = {}, Value ={}", consumerRecord.offset(),
							consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());					
					if ((Integer.parseInt(consumerRecord.key()) % 100000) ==  0) {
						log.info("Offset = {}, Particion = {}, Key = {}, Value ={}", consumerRecord.offset(),
								consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());	
					}
				}
					
			}
		}
			catch(WakeupException e) {
				if(!closed.get())
					throw e;
			}finally{
				consumer.close();
			}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

}
