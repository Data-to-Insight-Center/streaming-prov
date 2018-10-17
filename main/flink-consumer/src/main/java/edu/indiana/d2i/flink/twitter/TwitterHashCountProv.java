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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TwitterHashCountProv {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "local_consumer");
//        properties.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

        DataStream<String> twitterStream = env.addSource(new FlinkKafkaConsumer010<>(
                "tweets", new SimpleStringSchema(), properties));

        DataStream<Tuple3<String, Integer, String>> countsWithProv = twitterStream.flatMap(new Tokenizer());
        DataStream<String> prov = countsWithProv.flatMap(new ProvSplitter());
        DataStream<Tuple2<String, Integer>> counts = countsWithProv.flatMap(new CountSplitter())
                        .keyBy(0)
                        .sum(1).setParallelism(1);

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        // emit results
//        prov.writeAsText("file:///home/isuru/2018thesiswork/software/flink-1.6.0/output/prov").setParallelism(1);
        prov.addSink(new FlinkKafkaProducer010<>("tweets-prov", new SimpleStringSchema(), producerProperties));
//        prov.addSink(new FlinkKafkaProducer010<>("tweets-prov", new SimpleStringSchema(), producerProperties, new FlinkFixedPartitioner())).setParallelism(1);
        counts.writeAsText("file:///home/isuru/2018thesiswork/software/flink-1.6.0/output/bar").setParallelism(1);

        // execute program
        env.execute("Streaming Hash Count");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    private static final class Tokenizer implements FlatMapFunction<String, Tuple3<String, Integer, String>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\s+");
            String inputId = "flatMap-in-" + UUID.randomUUID();
            String invocationId = "flatMap-" + UUID.randomUUID();
            out.collect(new Tuple3<>(null, null, createEdge(invocationId, inputId, "used", 0)));

            // emit the pairs
            List<String> nots = new ArrayList<>();
            for (String token : tokens) {
                if (token.length() > 0 && token.startsWith("#")) {
                    nots.add(createEdge("flatMap-out-" + UUID.randomUUID(), invocationId, "wasGeneratedBy", 0));
                    out.collect(new Tuple3<>(token, 1, null));
                }
            }

            // emit provenance
            if (nots.size() > 0)
                out.collect(new Tuple3<>(null, null, createJSONArray(nots, "wasGeneratedBy", 0)));
        }
    }

    private static final class CountSplitter implements FlatMapFunction<Tuple3<String, Integer, String>, Tuple2<String, Integer>> {

        @Override
        public void flatMap(Tuple3<String, Integer, String> value, Collector<Tuple2<String, Integer>> out) {
            if (value.f0 != null)
                out.collect(new Tuple2<>(value.f0, value.f1));
        }
    }

    private static final class ProvSplitter implements FlatMapFunction<Tuple3<String, Integer, String>, String> {

        @Override
        public void flatMap(Tuple3<String, Integer, String> value, Collector<String> out) {
            if (value.f0 == null)
                out.collect(value.f2);
        }
    }

    private static String createEdge(String sourceId, String destId, String edgeType, int partition) {
        return "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
    }

    private static String createJSONArray(List<String> notifications, String edgeType, int partition) {
        if (notifications.size() == 1) {
            return notifications.get(0);
        } else if (notifications.size() > 1) {
            StringBuilder array = new StringBuilder("{\"group\":[");
            for (int i = 0; i < notifications.size(); i++) {
                array.append(notifications.get(i));
                if (i < notifications.size() - 1)
                    array.append(", ");
            }
            array.append("], \"edgeType\":\"").append(edgeType)
                    .append("\", \"partition\":\"").append(partition).append("\"}");
            return array.toString();
        }
        return null;
    }

}
