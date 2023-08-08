package com.kafka.project;

import com.google.gson.Gson;
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

public class CountTest3JsonFormat {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String GROUP_ID_CONFIG = "consumerGroup1";

    public static void main(String[] args) {
        Consumer<Long, String> consumer = createConsumer();
        Producer<String,String> producer = createProducer();
        Gson gson = new Gson();

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

            timeToSearch.clear();
            timeToSearch.put(topicPartition, todayMidnight.toInstant(ZoneOffset.UTC).toEpochMilli());
            long todayOffset = consumer.offsetsForTimes(timeToSearch).get(topicPartition).offset();

            long partitionMessageCount = todayOffset - previousDayOffset;

            totalMessageCount += partitionMessageCount;

            // Create a summary message and send to the summary topic
            Map<String, Object> summaryMap = new HashMap<>();
            summaryMap.put("topic", AppConfigs.SUMMARY_TOPIC);
            summaryMap.put("partition", partitionInfo.partition());
            summaryMap.put("previousDayTimestamp", previousDayMidnight.toString());
            summaryMap.put("previousDayOffset", previousDayOffset);
            summaryMap.put("currentTimestamp", todayMidnight.toString());
            summaryMap.put("currentDayOffset", todayOffset);
            summaryMap.put("messagesProcessedInADay", partitionMessageCount);
            String summaryMessage = gson.toJson(summaryMap);

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
