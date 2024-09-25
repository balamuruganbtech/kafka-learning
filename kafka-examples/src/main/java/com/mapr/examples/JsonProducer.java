package com.mapr.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.Properties;
import java.io.IOException;

public class JsonProducer
{
	public static void main(String[] args) {
		try {
			produce("192.168.227.128:9092","demo4");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public static void produce(String brokers, String topicName) throws IOException
    {

        // Set properties used to configure the producer
        Properties properties = new Properties();
        // Set the brokers (bootstrap servers)
        properties.setProperty("bootstrap.servers", brokers);
        // Set how to serialize key/value pairs
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.connect.json.JsonSerializer");
        // specify the protocol for SSL Encryption This is needed for secure clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        KafkaProducer producer = new KafkaProducer(properties);
        ObjectMapper objectMapper = new ObjectMapper();
        
        try {
            Contact contact = new Contact();
            contact.setContactId(1);
            contact.setFirstName("Bububombo");
            contact.setLastName("Tekateka");
            //contact.setDetail(Obh1);
            JsonNode jsonNode = objectMapper.valueToTree(contact);
            ProducerRecord rec = new ProducerRecord(topicName, jsonNode);
            producer.send(rec);
            System.out.println("record has been sent to kafka");
            
        } catch (Exception ex) {
        	ex.printStackTrace();
        } finally {
        	producer.close();
        }
    }
}