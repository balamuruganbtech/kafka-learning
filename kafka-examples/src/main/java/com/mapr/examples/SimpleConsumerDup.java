package com.mapr.examples;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumerDup {
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
      props.put("client.id", "client2");
      props.put("auto.offset.reset", "earliest ");
      props.put("max.partition.fetch.bytes","1048576");
      props.put("fetch.max.bytes","52428800");
      props.put("max.poll.records","10");
      props.put("request.timeout.ms","30000");
      props.put("session.timeout.ms","80000");
      
      props.put("enable.auto.commit", "false");
      
      props.put("auto.commit.interval.ms", "1000");
      
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(100);
         
         for (ConsumerRecord<String, String> record : records)
         
         // print the offset,key and value for the consumer records.
         System.out.printf("offset = %d, key = %s, value = %s\n", 
            record.offset(), record.key(), record.value());
        
         //consumer.seek(1, 12);
         consumer.commitAsync();
      }
      //consumer.seek(topicPartition, startingOffset);
      
   }
}