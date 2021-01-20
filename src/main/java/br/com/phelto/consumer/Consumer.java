package br.com.phelto.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
	
	public static void main(String[] args) {
		var properties = getProperties();
		try(var consumer = new KafkaConsumer<String, String>(properties);){
		
		consumer.subscribe(Collections.singletonList("LOJA_NOVO_PEDIDO"));
		
		while(true) {
			var records = consumer.poll(Duration.ofMillis(100));
			if(records.isEmpty()) {
				continue;
			}
			
			for (var record : records) {
				
				System.out.println("----------------------------------------");
				System.out.println(record.key());
				System.out.println(record.value());
				System.out.println(record.partition());
				System.out.println(record.offset());
			}
		}}catch(Exception ex) {
			
			throw ex;
		}
	}

	private static Properties getProperties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Consumer.class.getName());
		
		return properties;
	}

}
