package io.kafka.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.30.54.39:9092");


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

  //  create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

       properties.setProperty("batch.size","400");

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {

                //  create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_topic","hello world" + i);

//        send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e == null){
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        }else{
                            log.error("Error while producing", e);
                        }


                    }
                });
            }

            try{
                Thread.sleep(500);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }



//        tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //        flush and close the producer
        producer.close();
    }
}
