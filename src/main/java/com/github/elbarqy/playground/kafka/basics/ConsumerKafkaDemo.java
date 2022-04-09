package com.github.elbarqy.playground.kafka.basics;

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

public class ConsumerKafkaDemo {
    private final Logger logger = LoggerFactory.getLogger(ConsumerKafkaDemo.class);

    public static void main(String[] args) {
        new ConsumerKafkaDemo().run();
    }

    private ConsumerKafkaDemo() {

    }

    private void run() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupID = "Threaded_consumer2";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                topic,
                groupID,
                latch
        );
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.error("Application has Exited ");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application has been interrupted " + e);
        } finally {
            logger.info("Exiting");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(
                String bootstrapServer,
                String topic,
                String groupID,
                CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(properties);
            this.consumer.subscribe(Collections.singletonList(topic));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(
                                "Partition " + record.partition() +
                                        "Key: " + record.key() +
                                        " Value: " + record.value() +
                                        " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received signal to shutdown");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            //interrupt poll with a WakeUpException
            this.consumer.wakeup();
        }
    }
}
