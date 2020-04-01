package com.github.javaPractice.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


//Revisit this its not working....
public class ConsumerDemoWithThreads {

    public static void main(String[] args) throws InterruptedException {
        //Create Consumer
        new ConsumerDemoWithThreads().run();
    }

    private  ConsumerDemoWithThreads() {

    }
    private void run() throws InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch  = new CountDownLatch(1);
        logger.info("Creating the consumer thread.");
        Runnable myConsumerThread = new ConsumerThread(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
        }

        ));

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interupted: " + e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;


        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);


        public ConsumerThread(CountDownLatch latch,
                              String bootstrapServers,
                              String groupId,
                              String topic) {

            this.latch = latch;
            //create consumer
            Properties properties = new Properties();
            consumer = new KafkaConsumer<String, String>(properties);

            //Subscribe consumer to our topics
            consumer.subscribe(Collections.singleton(topic));

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        public ConsumerThread(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
        }

        @Override
        public void run() {
            //poll for our data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll
                            (Duration.ofMillis(100)); //new in kafka 2.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value() + "\n" +
                                "Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown Singal!");
            } finally {
                consumer.close();
                // tell the main code that we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // special method to interrupt consumer.poll()
            // it will throw the exception "wakeup"
            consumer.wakeup();
        }
    }
}
