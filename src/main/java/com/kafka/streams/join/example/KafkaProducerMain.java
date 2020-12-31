package com.kafka.streams.join.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

public class KafkaProducerMain {
	public static void main(String[] args) {
		KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
		
		// left topic
		// producer.send(new ProducerRecord<String, String>("left-input", "a", "value1"));
		// producer.send(new ProducerRecord<String, String>("left-input", "b", "value1"));
		
		// right topic
		// producer.send(new ProducerRecord<String, String>("right-input", "b", "value1"));
		producer.send(new ProducerRecord<String, String>("right-input", "a", "value-right-2"));
		producer.close();
	}
	
	public static Properties getProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
		return props;
	}
}
