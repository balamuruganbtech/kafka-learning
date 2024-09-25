//package com.mapr.examples;
//
//import java.util.List;
//
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
//public class KafkaTransactionsExample {
//	 
//	  public static void main(String args[]) {
//	    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
//
//
//	    // Note that the ‘transaction.app.id’ configuration _must_ be specified in the 
//	    // producer config in order to use transactions.
//	    KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
//
//	    // We need to initialize transactions once per producer instance. To use transactions, 
//	    // it is assumed that the application id is specified in the config with the key 
//	    // transactions.app.id.
//	    // 
//	    // This method will recover or abort transactions initiated by previous instances of a 
//	    // producer with the same app id. Any other transactional messages will report an error 
//	    // if initialization was not performed.
//	    //
//	    // The response indicates success or failure. Some failures are irrecoverable and will 
//	    // require a new producer  instance. See the documentation for TransactionMetadata for a 
//	    // list of error codes.
//	    producer.initTransactions();
//	    
//	    while(true) {
//	      ConsumerRecords<String, String> records = consumer.poll(100);
//	      if (!records.isEmpty()) {
//	        // Start a new transaction. This will begin the process of batching the consumed 
//	        // records as well
//	        // as an records produced as a result of processing the input records. 
//	        //
//	        // We need to check the response to make sure that this producer is able to initiate 
//	        // a new transaction.
//	        producer.beginTransaction();
//	        
//	        // Process the input records and send them to the output topic(s).
//	        List<ProducerRecord<String, String>> outputRecords = processRecords(records);
//	        for (ProducerRecord<String, String> outputRecord : outputRecords) {
//	          producer.send(outputRecord); 
//	        }
//        
//	        // To ensure that the consumed and produced messages are batched, we need to commit 
//	        // the offsets through
//	        // the producer and not the consumer.
//	        //
//	        // If this returns an error, we should abort the transaction.
//	        
//	        //sendOffsetsResult = producer.sendOffsets(getUncommittedOffsets());
//	        
//	     
//	        // Now that we have consumed, processed, and produced a batch of messages, let's 
//	        // commit the results.
//	        // If this does not report success, then the transaction will be rolled back.
//	        producer.endTransaction();
//	      }
//	    } 
//	  }
//	}
//
//
//
//
//
////KafkaProducer producer = createKafkaProducer(
////  “bootstrap.servers”, “localhost:9092”,
////  “transactional.id”, “my-transactional-id”);
////
////producer.initTransactions();
////
////KafkaConsumer consumer = createKafkaConsumer(
////  “bootstrap.servers”, “localhost:9092”,
////  “group.id”, “my-group-id”,
////  "isolation.level", "read_committed");
////
////consumer.subscribe(singleton(“inputTopic”));
////
////while (true) {
////  ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
////  producer.beginTransaction();
////  for (ConsumerRecord record : records)
////    producer.send(producerRecord(“outputTopic”, record));
////  producer.sendOffsetsToTransaction(currentOffsets(consumer), group);  
////  producer.commitTransaction();
////}