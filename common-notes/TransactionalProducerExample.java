package com.mapr.examples;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;


public class TransactionalProducerExample {

  public static void main(String[] args) {
    
    

      /* parse args */
      String brokerList = "localhost:9092,localhost:9093,localhost:9094,localhost:9095";
      String topic = "sales2";
      long noOfMessages = 500;
      long delay = 100L;
      //String messageType = res.getString("messagetype");


      Properties producerConfig = new Properties();
      producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
      producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer1");
      producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
      producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transactional-id1"); // set transaction id
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

     
      Producer<String, String> producer = new KafkaProducer<String, String>(
    		  producerConfig);
      producer.initTransactions(); //initiate transactions
      System.out.println("before try block");
      try {
        producer.beginTransaction(); //begin transactions
        System.out.println("after begin");
        for (int i = 0; i < noOfMessages; i++) {
        	 System.out.println("in the for loop");
			producer
			.send(new ProducerRecord<String, String>(topic, Integer
					.toString(i), Integer.toString(i)),new MyProducerCallback());
			 System.out.println("After send()");

          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
        	  System.out.println("in the catch"+e.getMessage());
          }
        }
        producer.commitTransaction(); //commit

      } catch (KafkaException e) {
        // For all other exceptions, just abort the transaction and try again.
        producer.abortTransaction();
      }

      producer.close();
    

  
  }


    

}