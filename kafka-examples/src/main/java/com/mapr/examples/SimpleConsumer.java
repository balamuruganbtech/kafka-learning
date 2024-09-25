package com.mapr.examples;

import java.time.Duration;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumer{
   public static void main(String[] args) throws Exception {
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      //Kafka consumer configuration settings
      String topicName = args[0].toString();
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "192.168.227.128:9092");
      props.put("group.id", "walmartJavaApp2");
      props.put("client.id", "client1");
      props.put("client.rack", "RC2");
      
      props.put("auto.offset.reset", "earliest");
      //props.put("enable.auto.commit", "false");
      
      //props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
//      KafkaConsumer<String, String> consumer1 = new KafkaConsumer
//    	         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      //TopicPartition obj = new TopicPartition("demo",0);
      //consumer.assign(arg0);
      //consumer.seek(obj, 63);
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//         ConsumerRecords<String, String> records1 = consumer.poll(100);
         
         //consumer.seek()
         
         
         for (ConsumerRecord<String, String> record : records){
         
         // print the offset,key and value for the consumer records.
         System.out.printf("partitionNo= %d, offset = %d, key = %s, value = %s\n", 
        		 record.partition(), record.offset(), record.key(), record.value());
        
         //consumer.seek(1, 12);
         consumer.commitAsync();
         }
         
//         for (ConsumerRecord<String, String> record1 : records){
//             
//             // print the offset,key and value for the consumer records.
//             System.out.printf("offset = %d, key = %s, value = %s\n", 
//                record1.offset(), record1.key(), record1.value());
//            
//             //consumer.seek(1, 12);
//             consumer1.commitAsync();
//             }
      }
//      consumer.seek(topicPartition, startingOffset);
//      
//      while (true) {
//          ConsumerRecords<String, String> records = consumer.poll(100);
//          for (ConsumerRecord<String, String> record : records) {
//              System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//          }
      
   }
}