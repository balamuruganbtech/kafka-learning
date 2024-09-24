package com.mapr.examples;

//import util.properties packages
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;



//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//Create java class named “SimpleProducer”
public class SimpleProducer{

	public static final String COMPRESSION_TYPE_CONFIG = "compression.type";

	public static void main(String[] args) throws Exception {

		// Check arguments length value
		if (args.length == 0) {
			System.out.println("Enter topic name");
			return;
		}
		
//		public void run(){  
//			System.out.println("thread is running...");  
//			}  

		// Assign topicName to string variable
		String topicName = args[0].toString();
		System.out.println(topicName);
		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "192.168.227.128:9092");

		// Set acknowledgements for producer requests.
		//min.insync.replicas=2
		
		props.put("acks", "all");

		//props.put("retry.backoff.ms", 5000);
        //gzip,snappy,LZO,LZ4,LZF
		props.put("compression.type", "snappy");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 2);
		props.put("retry.backoff.ms",3000);
		
		props.put("batch.size",16384);

		// Specify buffer size in config
		
		
		props.put("client.id","walmartApp2");
		
		//The value of this config should be greater than or equal to the sum of request.timeout.ms and linger.ms
		props.put("delivery.timeout.ms", 20000);
		
		props.put("max.in.flight.requests.per.connection",5);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 10);

		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		
		//props.put("enable.idempotence",true);
		
		props.put("request.timeout.ms","10000");

		Producer<String, String> producer = new KafkaProducer<String, String>(
				props);
		System.out.println("Message sent successfully before for loop");
		List words = new ArrayList();
		words.add("i am mukesh");
		words.add("i am ravi");
		words.add("i am kishore");

		int counter=0;
		
		for (int i = 600; i < 700; i++) {
			
			//fire nad notify
			
			
			if(counter > 2){
				counter=0;
			}
//			 producer.send(new ProducerRecord<String, String>(topicName,
//			 Integer.toString(i), Integer.toString(i)),new
//			 MyProducerCallback());
			//fire and forget
//			producer
//			.send(new ProducerRecord<String, String>(topicName, Integer
//					.toString(i), Integer.toString(i)));
//			//fire &  notify
//			producer
//			.send(new ProducerRecord<String, String>(topicName, Integer
//					.toString(i), Integer.toString(i)),new MyProducerCallback());
			
			producer
			.send(new ProducerRecord<String, String>(topicName, Integer.toString(i),(String)words.get(counter)),new MyProducerCallback());
			
			counter++;
			
//			producer
//			.send(new ProducerRecord<String, String>(topic, partition, key, value)),new MyProducerCallback());
			
			//sync
//			Future<RecordMetadata> rd = producer
//					.send(new ProducerRecord<String, String>(topicName, Integer
//							.toString(i), Integer.toString(i)));
//
// 			System.out.println(rd.get());
// 			
 			
// 			producer
//			.send(new ProducerRecord<String, String>(topicName, 0, "110", "I am Mukesh"));
		}
		System.out.println("Message sent successfully inside loop");
		producer.close();
	}
}
