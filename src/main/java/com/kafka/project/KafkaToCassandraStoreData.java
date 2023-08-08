package com.kafka.project;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class KafkaToCassandraStoreData {

    private static  final Logger logger= LogManager.getLogger();
    public static void main(String[] args) {
        // Kafka configuration
        logger.info("Kafka To Cassandra Database Program");
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("group.id", "my-consumer-group");

        // Cassandra configuration
        logger.info("Connecting to Cassandra Database");
        Cluster cluster = Cluster.builder()
                .addContactPoint("localhost")
                .build();
        Session session = cluster.connect("testdata");

        logger.info("Successfully Connected to Cassandra Database");

        // Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(AppConfigs.topicName));

        // Consume and store messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // Extract data from Kafka message
                String key = record.key();
                String value = record.value();
                // Insert data into Cassandra
                String insertQuery = "INSERT INTO messages (message_id,message_name) VALUES (?, ?)";
                session.execute(insertQuery,key,value);
            }
            logger.info("Sending messages to Cassandra");
            logger.info("successfully Completed");
        }

    }
}

