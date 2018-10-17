package edu.indiana.d2i.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TweetPublisher {

    private Producer<String, String> kafkaProducer;

    public TweetPublisher() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", 50000); // in bytes
        props.put("buffer.memory", 1000000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        TweetPublisher publisher = new TweetPublisher();
        publisher.publish();
//        publisher.hashCount();
    }

    public void hashCount() {
        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/hashb-input/tweets-16m";
//        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/mini-input/mini";
//        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/hash100-input/tweets-100";

        try {
            File file = new File(tweetFilePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            int tweetCount = 0;
            Map<String, Integer> hashCounts = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                tweetCount++;
                String[] tokens = line.toLowerCase().split("\\s+");
                for (String token : tokens)
                    if (token.length() > 0 && token.startsWith("#")) {
                        Integer current = hashCounts.get(token);
                        hashCounts.put(token, current == null ? 1 : current + 1);
                    }
            }
            fileReader.close();
            StringBuilder buff = new StringBuilder();
            for (String k : hashCounts.keySet()) {
                buff.append(k).append(", ").append(hashCounts.get(k)).append("\n");
//                System.out.println(k + ", " + hashCounts.get(k));
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter("/home/isuru/2018thesiswork/temp/hashout"));
            writer.write(buff.toString());
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void publish() {
//        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/mini-input/mini";
        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/hashb-input/tweets-16m";
//        String tweetFilePath = "/home/isuru/2018thesiswork/software/hadoop-2.8.1/hash100-input/tweets-100";

        try {
            File file = new File(tweetFilePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            int tweetCount = 0;
            while ((line = bufferedReader.readLine()) != null) {
                tweetCount++;
                kafkaProducer.send(new ProducerRecord<>("tweets", tweetCount % 3, "line", line));
            }
            fileReader.close();
            System.out.println("Tweets sent: " + tweetCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
    }

}
