package br.com.phelto.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NovoPedido {
	

	public static void main(String[] args) throws InterruptedException, ExecutionException {
	
		Properties properties = getProperties();
		
		var producer = new KafkaProducer<String, String>(properties);
		
		var producerRecord = new ProducerRecord<>("LOJA_NOVO_PEDIDO", "TESTE_CHAVE","TESTE_VALOR");
		
		producer.send(producerRecord, (data, ex) -> {
				if(ex != null ) {
					ex.printStackTrace();
					return;
				}
				
				System.out.println(data.topic());
			
		}).get();
		
		producer.close();
		
		
	}

	private static Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
