package com.kafka.project;

import org.apache.kafka.clients.consumer.ConsumerRecord;
        import org.apache.kafka.clients.consumer.ConsumerRecords;
        import org.apache.kafka.clients.consumer.KafkaConsumer;

        import java.time.Duration;
        import java.util.Arrays;
        import java.util.Properties;

public class KafkaHtml {

    public static void main(String[] args) {

        // Kafka consumer configuration settings

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Arrays.asList(AppConfigs.SOURCE_TOPIC));

        System.out.println("<html>\n<body>\n<ul>");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {

                    System.out.println("<li>" + record.value() + "</li>");

                }
            }
        } finally {
            consumer.close();
            System.out.println("</ul>\n</body>\n</html>");
        }
    }
}

