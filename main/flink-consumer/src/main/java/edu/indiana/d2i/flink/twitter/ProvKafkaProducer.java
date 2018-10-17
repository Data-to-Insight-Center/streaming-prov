/*
 * Copyright 2017 The Trustees of Indiana University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author isuriara@indiana.edu
 */

package edu.indiana.d2i.flink.twitter;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ProvKafkaProducer {

    private static ProvKafkaProducer provProducer;
    private Producer<String, String> kafkaProducer;
//    //    private Producer<String, JsonNode> kafkaProducer;
    private static String kafkaTopic;
//    private static String partitionStrategy;
//    //    private static final String kafkaTopic = "mr-prov";
////    private static Properties producerProperties;
//    private static int numberOfPartitions;
//    private static int partitionToWrite;
//    public int messageCount = 0;

    private ProvKafkaProducer() {
        kafkaTopic = "tweets-prov";
//        loadPropertiesFromFile();
//        kafkaTopic = producerProperties.getProperty("kafka.topic");
//        partitionStrategy = producerProperties.getProperty("partition.strategy");
//        partitionToWrite = Integer.parseInt(producerProperties.getProperty("partition.to.write"));
//        numberOfPartitions = Integer.parseInt(producerProperties.getProperty("number.of.partitions"));
//        System.out.println("#### properties read, partition to write = " + partitionToWrite);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("bootstrap.servers", producerProperties.getProperty("bootstrap.servers"));
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
        props.put("batch.size", 50000); // in bytes
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("buffer.memory", 1000000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("key.serializer", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
//        props.put("value.serializer", "org.apache.kafka.connect.json.JsonConverter");

        kafkaProducer = new KafkaProducer<>(props);

    }

    public static int getNumberOfPartitions() {
        return 3;
//        return numberOfPartitions;
    }
//
//    public static int getPartitionToWrite() {
//        return partitionToWrite;
//    }
//
//    public static String getPartitionStrategy() {
//        return partitionStrategy;
//    }

//    private void loadPropertiesFromFile() {
//        producerProperties = new Properties();
//        try {
////            producerProperties.load(new FileInputStream("/home/isurues/hadoop/kafka.properties"));
//            producerProperties.load(new FileInputStream("/home/isuru/2018thesiswork/checkouts/streaming-prov/kafka-producer/kafka.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static synchronized ProvKafkaProducer getInstance() {
        if (provProducer == null) {
            provProducer = new ProvKafkaProducer();
        }
        return provProducer;
    }

    public void close() {
        kafkaProducer.close();
    }

    public void createAndSendEdge(String sourceId, String destId, String edgeType, int partition) {
        String notification = "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notification));
    }

    public String createEdge(String sourceId, String destId, String edgeType, int partition) {
        return "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
    }

    public void createAndSendJSONArray(List<String> notifications, String edgeType, int partition) {
        if (notifications.size() == 1) {
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notifications.get(0)));
        } else if (notifications.size() > 1) {
            StringBuilder array = new StringBuilder("{\"group\":[");
            for (int i = 0; i < notifications.size(); i++) {
                array.append(notifications.get(i));
                if (i < notifications.size() - 1)
                    array.append(", ");
            }
            array.append("], \"edgeType\":\"").append(edgeType)
                    .append("\", \"partition\":\"").append(partition).append("\"}");
            String notification = array.toString();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notification));
        }
    }

}
