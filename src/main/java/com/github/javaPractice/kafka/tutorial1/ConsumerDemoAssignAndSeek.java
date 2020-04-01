package com.github.javaPractice.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);;

        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";

        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and seek are mostly used to replay data or fetch a specific message
        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetsToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetsToReadFrom);

        int numMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        //poll for our data

        while(keepOnReading) {
            ConsumerRecords<String,String> records = consumer.poll
                    (Duration.ofMillis(100)); //new in kafka 2.0

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesReadSoFar >= numMessagesToRead) {
                    keepOnReading = false; //exit while loop
                    break;
                }
            }
        }
        logger.info("Exiting the application.");
    }
}
