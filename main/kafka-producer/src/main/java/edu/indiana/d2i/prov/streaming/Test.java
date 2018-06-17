package edu.indiana.d2i.prov.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Test {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int count = 0;
        Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i = 0; i < 30; i++) {
            String message = "{\"message\" : \"test " + Integer.toString(i) + "\"}";
//            System.out.println(message);
            kafkaProducer.send(new ProducerRecord<>("test-p3", count++ % 3, "line", message));
        }
        kafkaProducer.close();

    }

}
