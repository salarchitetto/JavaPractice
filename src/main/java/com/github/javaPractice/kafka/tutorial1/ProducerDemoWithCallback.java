package com.github.javaPractice.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        //Creates a logger for this class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
        final KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);
        //Key to be a string, value to be a string ^^

        for (int i = 0; i < 10; i++ ) {
        //Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                "This is a test: " + Integer.toString(i));


            //send data - async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //then the record was sent
                        logger.info("received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("While while producing data: " + e);
                    }
                }
            });
        }

        //Forces the data to be produced
        producer.flush();
        //Flush and close producer
        producer.close();

    }
}



