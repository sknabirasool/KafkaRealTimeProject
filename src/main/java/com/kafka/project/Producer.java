package com.kafka.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class Producer {
    private static  final Logger logger= LogManager.getLogger();
    public static void main(String[] args) {

        logger.info("Kafka Producer Starting");
        Properties props=new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer,String> producer=new KafkaProducer<>(props);
        logger.info("Start Sending Messages");
        int messagecount=0;
        for (int i=0;i<AppConfigs.numEvents;i++)
        {
            producer.send(new ProducerRecord<>(AppConfigs.SOURCE_TOPIC,i,"Simple Message "+i));
            messagecount++;
        }
        logger.info("Finished Sending Messages");
        logger.info("sum is"+messagecount);
        producer.close();
    }
}
