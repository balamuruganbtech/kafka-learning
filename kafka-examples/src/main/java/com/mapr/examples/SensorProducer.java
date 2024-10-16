package com.mapr.examples;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SensorProducer {
                                
public static void main(String[] args) throws Exception{
                                
    String topicName = "sensor";
                                
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.227.128:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", SensorPartitioner.class.getName());
        props.put("speed.sensor.name", "TSS");
                                    
        Producer<String, String> producer = new KafkaProducer<>(props);
                                    
        for (int i=0 ;i<100 ; i++)
            producer.send(new ProducerRecord<>(topicName,"SSP","500"+i));
        //producer.send(new ProducerRecord<String, String>(topic, partition, timestamp, key, value));
                                    
        for (int i=0 ;i<100 ; i++)
            producer.send(new ProducerRecord<>(topicName,"TSS","500"+i));
        
        
                                    
        producer.close();                                        
        System.out.println("SimpleProducer Completed.");
    }
}                                               
      