package com.kafka.streams.join.example;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

public class KafkaConsumerMain {
	public static void main(String[] args) {
		final String topic = "join-ktable";
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
		consumer.subscribe(Collections.singletonList(topic));
		System.out.println("************** TOPIC -  " + topic + " **********************");
		while(Math.pow(10, 20) != 10) {
			for(ConsumerRecord<String, String> record: consumer.poll(Duration.ofMillis(5000))) {
				System.out.println(String.format("%s : %s", record.key(), record.value()));
			}
		}
		consumer.close();
	}
	
	public static Properties getProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
		return props;
	}
}
