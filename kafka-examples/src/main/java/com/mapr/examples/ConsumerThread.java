package com.mapr.examples;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerThread {	
	

	public static void main(String[] args) throws Exception  {
		Properties configProperties = new Properties();
						
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
		configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		// Figure out where to start processing messages from
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		String topicName= args[0];
		final int startingOffset = Integer.valueOf(args[2]);
		
		
		kafkaConsumer.subscribe(Arrays.asList(topicName),
				new ConsumerRebalanceListener() {
					public void onPartitionsRevoked(
							Collection<TopicPartition> partitions) {
						System.out
								.printf("%s topic-partitions are revoked from this consumer\n",
										Arrays.toString(partitions.toArray()));
					}

					public void onPartitionsAssigned(
							Collection<TopicPartition> partitions) {
						System.out
								.printf("%s topic-partitions are assigned to this consumer\n",
										Arrays.toString(partitions.toArray()));
						Iterator<TopicPartition> topicPartitionIterator = partitions
								.iterator();
						while (topicPartitionIterator.hasNext()) {
							TopicPartition topicPartition = topicPartitionIterator
									.next();
							System.out.println("Current offset is "
									+ kafkaConsumer.position(topicPartition)
									+ " committed offset is ->"
									+ kafkaConsumer.committed(topicPartition));

							System.out.println("Resetting offset to "
									+ startingOffset);
							kafkaConsumer.seek(topicPartition, startingOffset);
						}
					}

				});
		// Start processing messages
		try {
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer
						.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
				}

			}
		} catch (WakeupException ex) {
			System.out.println("Exception caught " + ex.getMessage());
		} finally {
			kafkaConsumer.close();
			System.out.println("After closing KafkaConsumer");
		}
	}

	
	
}