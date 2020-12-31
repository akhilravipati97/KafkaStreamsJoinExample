package com.kafka.streams.join.example;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class KafkaStreamsJoinExampleApplication {

	public static void main(String[] args) {
		join();
	}
	
	private static void join() {
		// must use a single builder for joins
		StreamsBuilder builder = new StreamsBuilder();
		
		// create the left ktable
		KTable<String, String> leftKTable = builder.table(
				"left-input", 
				Consumed.with(Serdes.String(), Serdes.String()),
				Materialized.as("left-ktable"));
		
		// create the right ktable
		KTable<String, String> rightKTable = builder.table(
				"right-input", 
				Consumed.with(Serdes.String(), Serdes.String()),
				Materialized.as("right-ktable"));
		
		// create the join - where are the unfinished joins stored?
		leftKTable.join(
				rightKTable, 
				(leftVal, rightVal) -> "left val: " + leftVal + ", right val:" + rightVal,
				Materialized.as("join-ktable"));
		
		// start the stream
		Topology topology = builder.build();
		System.out.println(topology.describe());
		KafkaStreams stream = new KafkaStreams(topology, getProperties());
		stream.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> stream.close()));
		
		// try to obtain the read only key-value store for the ktable
		ReadOnlyKeyValueStore<String, String> store = null;
		while(store == null) {
			try {
				Thread.sleep(1000);
				store = stream.store("join-ktable", QueryableStoreTypes.keyValueStore());
			} catch (Exception e) {
				System.out.println("**************Will try again");
			}
		}
		KeyValueIterator<String, String> itr = store.all();
		while(itr.hasNext()) {
			KeyValue<String, String> kv = itr.next();
			System.out.println("***Inside the joined state store -------- key: " + kv.key + " **********");
			System.out.println("***Inside the joined state store -------- value: " + kv.value + " **********");
		}
		
			   
	}
	
	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka.streams.join.example");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
		props.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\akhil\\AppData\\Local\\kafka_2.12-2.3.0\\streams");
		return props;
	}

}
