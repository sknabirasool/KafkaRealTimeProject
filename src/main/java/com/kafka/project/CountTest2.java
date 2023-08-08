package com.kafka.project;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class CountTest2 {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String GROUP_ID_CONFIG = "consumerGroup1";

    public static void main(String[] args) {
        Consumer<Long, String> consumer = createConsumer();
        Producer<String, String> producer = createProducer();

        // Fetch partition information for the topic
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(AppConfigs.SOURCE_TOPIC);

        // Define your time range
        LocalDateTime previousDayMidnight = LocalDate.now().minusDays(1).atStartOfDay();
        LocalDateTime todayMidnight = LocalDate.now().atStartOfDay();

        long totalMessageCount = 0;
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(AppConfigs.SOURCE_TOPIC, partitionInfo.partition());
            consumer.assign(Collections.singletonList(topicPartition));

            Map<TopicPartition, Long> timeToSearch = new HashMap<>();
            timeToSearch.put(topicPartition, previousDayMidnight.toInstant(ZoneOffset.UTC).toEpochMilli());
            long previousDayOffset = consumer.offsetsForTimes(timeToSearch).get(topicPartition).offset();
            System.out.println("Previous day midnight offset for partition " + partitionInfo.partition() + ": " + previousDayOffset);

            timeToSearch.clear();
            timeToSearch.put(topicPartition, todayMidnight.toInstant(ZoneOffset.UTC).toEpochMilli());
            long todayOffset = consumer.offsetsForTimes(timeToSearch).get(topicPartition).offset();
            System.out.println("Today midnight offset for partition " + partitionInfo.partition() + ": " + todayOffset);

            long partitionMessageCount = todayOffset - previousDayOffset;
            System.out.println("Message count for partition " + partitionInfo.partition() + ": " + partitionMessageCount);

            totalMessageCount += partitionMessageCount;

            // Send a summary message to the summary topic
            String summaryMessage = String.format(
                    "Topic: %s, Partition: %d, Previous day timestamp: %s, Previous day offset: %d, " +
                            "Current timestamp: %s, Current day offset: %d, Messages processed in a day: %d",
                    AppConfigs.SOURCE_TOPIC, partitionInfo.partition(), previousDayMidnight, previousDayOffset,
                    todayMidnight, todayOffset, partitionMessageCount);
            System.out.println(summaryMessage);
            producer.send(new ProducerRecord<>(AppConfigs.SUMMARY_TOPIC, Integer.toString(partitionInfo.partition()), summaryMessage));
        }

        System.out.println("Total message count: " + totalMessageCount);

        consumer.close();
        producer.close();
    }

    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
