package com.github.javaPractice.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        String boostrapSevers = "127.0.0.1:9092";

        //Create producer properties

        //Creates new properties object
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapSevers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        //What type of values are you sending to Kafka
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);
        //Key to be a string, value to be a string ^^

        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                "Hello, World!");


        //send data - async
        producer.send(record);

        //Forces the data to be produced
        producer.flush();
        //Flush and close producer
        producer.close();

    }
}
