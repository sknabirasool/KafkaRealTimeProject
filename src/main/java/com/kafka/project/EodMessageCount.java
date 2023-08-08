package com.kafka.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.*;
import java.util.Collections;
import java.util.Properties;

public class EodMessageCount {
    private  static  final Logger logger= LogManager.getLogger();
    public static void main(String[] args) {

        String brokerAddress = "localhost:9092"; // replace with your broker address
        String inputTopic = "your_input_topic"; // replace with your input topic name
        String outputTopic = "Eddas-day-count"; // output topic name

        // Configure Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Configure Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka consumer and producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Subscribe to the input topic
        consumer.subscribe(Collections.singletonList(AppConfigs.topicName));

        // Calculate the timestamp for the previous day's midnight and today's midnight
        Instant startInstant = LocalDateTime.now().minusDays(1).withHour(0).withMinute(0).withSecond(0).atZone(ZoneId.systemDefault()).toInstant();
        Instant endInstant = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).atZone(ZoneId.systemDefault()).toInstant();

        System.out.println(startInstant);
        System.out.println(endInstant);
        // Count variable to track the number of messages
        int messageCount = 0;

        // Poll for new messages and increment the count if timestamp is within the specified range
      logger.info("Started");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                long timestamp = record.timestamp();

                if (timestamp >= startInstant.toEpochMilli() && timestamp < endInstant.toEpochMilli()) {
                    messageCount++;
                    System.out.println(messageCount);
                }
            }

            // If any messages counted, then write the count to the output topic and break
            if (messageCount > 0) {
                ProducerRecord<String, String> outRecord = new ProducerRecord<>(AppConfigs.topicName1, "count", Integer.toString(messageCount));
                producer.send(outRecord);
                logger.info("------------");
                System.out.println(outRecord);
                System.out.println(messageCount);
                break;  // If you want to continuously keep polling, remove this line
            }
        }
        logger.info("closed");

        producer.close();
        consumer.close();
    }
}
