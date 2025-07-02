package io.kafka.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "demo_topic_app";

        String topic = "demo_topic";


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.30.54.39:9092");


        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        get a refernce to the main thread
        final Thread mainThread = Thread.currentThread();

//        adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
//        subscribe consumer
        consumer.subscribe(Arrays.asList(topic));


        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }catch (WakeupException e){
            log.info("consumer is starting to shut down");
        }catch (Exception e){
            log.error("Unexpected exception in the consumer", e);
        }
        finally {
            consumer.close();
            log.info("shutdown");
        }

    }
}
