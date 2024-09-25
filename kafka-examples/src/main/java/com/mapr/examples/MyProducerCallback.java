package com.mapr.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		if (e != null){
			System.out.println("AsynchronousProducer failed with an exception::"+e.getMessage());
		
			System.out.println("failed string :"+recordMetadata.toString());
			System.out.println("failed call for topic:"+recordMetadata.topic());
			System.out.println("failed call at partition:"+recordMetadata.partition());
		}
		else
		{
			
			System.out.println("AsynchronousProducer call Success string :"+recordMetadata.toString());
			System.out.println("AsynchronousProducer call Success for topic:"+recordMetadata.topic());
			System.out.println("AsynchronousProducer call Success at partition:"+recordMetadata.partition());
			System.out.println("AsynchronousProducer call Success at offset:"+recordMetadata.offset());
			System.out.print("AsynchronousProducer call Success at offset:"+recordMetadata.timestamp());
			
		}
	}
}