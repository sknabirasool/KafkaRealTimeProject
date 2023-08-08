package com.kafka.project;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class CountOffset {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String GROUP_ID_CONFIG = "consumerGroup1";

    public static void main(String[] args) {
        Consumer<Long, String> consumer = createConsumer();

        // Fetch partition information for the topic
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(AppConfigs.topicName);
        List<Integer> partitionNumbers = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            partitionNumbers.add(partitionInfo.partition());
        }

        // Define your time range
        LocalDateTime previousDayMidnight = LocalDate.now().minusDays(1).atStartOfDay();
        LocalDateTime todayMidnight = LocalDate.now().atStartOfDay();

        System.out.println(previousDayMidnight);
        System.out.println(todayMidnight);

        long totalMessageCount = 0;
        for (Integer partitionNumber : partitionNumbers) {
            TopicPartition topicPartition = new TopicPartition(AppConfigs.topicName, partitionNumber);
            consumer.assign(Collections.singletonList(topicPartition));

            Map<TopicPartition, Long> timeToSearch = new HashMap<>();
            timeToSearch.put(topicPartition, previousDayMidnight.toInstant(ZoneOffset.UTC).toEpochMilli());
            long previousDayOffset = consumer.offsetsForTimes(timeToSearch).get(topicPartition).offset();
            System.out.println("Previous day midnight offset for partition " + partitionNumber + ": " + previousDayOffset);

            timeToSearch.clear();
            timeToSearch.put(topicPartition, todayMidnight.toInstant(ZoneOffset.UTC).toEpochMilli());
            long todayOffset = consumer.offsetsForTimes(timeToSearch).get(topicPartition).offset();
            System.out.println("Today midnight offset for partition " + partitionNumber + ": " + todayOffset);

            long partitionMessageCount = todayOffset - previousDayOffset;
            System.out.println("Message count for partition " + partitionNumber + ": " + partitionMessageCount);

            totalMessageCount += partitionMessageCount;
        }

        System.out.println("Total message count: " + totalMessageCount);

        consumer.close();
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
}
