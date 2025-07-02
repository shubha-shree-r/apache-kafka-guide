package io.kafka.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.30.54.39:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

  //  create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //  create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_topic","hello world");

//        send data
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully");
            } else {
                exception.printStackTrace();
            }
        });


//        tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //        flush and close the producer
        producer.close();
    }
}
