package com.github.javaPractice.kafka.twitterKafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Properties;

public class TwitterConsumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);
        TwitterConfigs configs = new TwitterConfigs();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.BOOTSTRAP_SERVERS);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configs.GROUP_ID);

//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(configs.TOPIC));

        while(true) {
            ConsumerRecords<Long,String> records = consumer.poll(1000); //new in kafka 2.0

            for (ConsumerRecord<Long, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value() + "\n" +
                        "Partition: " + record.partition());
            }
        }

    }
}

