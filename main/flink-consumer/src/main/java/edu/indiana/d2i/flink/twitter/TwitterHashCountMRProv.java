package edu.indiana.d2i.flink.twitter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TwitterHashCountMRProv {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "local_consumer");

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<String> twitterStream = env.addSource(new FlinkKafkaConsumer010<>(
                "tweets", new SimpleStringSchema(), properties));

        DataStream<WordCount> tokensWithProv = twitterStream.flatMap(new Tokenizer());
        DataStream<String> prov = tokensWithProv.flatMap(new ProvSplitter());
        DataStream<String> counts = tokensWithProv.flatMap(new CountSplitter())
                .keyBy("word")
                .reduce(new CountReducer())
                .flatMap(new CountEmitter());

        // emit result
        prov.addSink(new FlinkKafkaProducer010<>("tweets-prov", new SimpleStringSchema(), producerProperties));
        counts.writeAsText("file:///home/isuru/2018thesiswork/software/flink-1.6.0/output/bar").setParallelism(1);
//        counts.print();

        // execute program
        env.execute("Streaming Hash Count");
    }


    private static final class Tokenizer implements FlatMapFunction<String, WordCount> {

        @Override
        public void flatMap(String value, Collector<WordCount> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\s+");
            String inputId = "Tokenizer-in-" + UUID.randomUUID();
            String invocationId = "Tokenizer-" + UUID.randomUUID();
            out.collect(new WordCount(createEdge(invocationId, inputId, "used", 0)));

            // emit the pairs
            List<String> nots = new ArrayList<>();
            for (String token : tokens) {
                if (token.length() > 0 && token.startsWith("#")) {
                    String outputId = "Tokenizer-out-" + UUID.randomUUID();
                    nots.add(createEdge(outputId, invocationId, "wasGeneratedBy", 0));
                    out.collect(new WordCount(token, 1L, outputId));
                }
            }

            // emit provenance
            if (nots.size() > 0)
                out.collect(new WordCount(createJSONArray(nots, "wasGeneratedBy", 0)));
        }
    }

    private static final class CountSplitter implements FlatMapFunction<WordCount, WordCount> {
        @Override
        public void flatMap(WordCount value, Collector<WordCount> out) {
            if (value.word != null)
                out.collect(value);
        }
    }

    private static final class ProvSplitter implements FlatMapFunction<WordCount, String> {
        @Override
        public void flatMap(WordCount value, Collector<String> out) {
            if (value.prov != null)
                out.collect(value.prov);
        }
    }

    private static final class CountReducer implements ReduceFunction<WordCount> {
        @Override
        public WordCount reduce(WordCount a, WordCount b) throws Exception {
            List<String> nots = new ArrayList<>();
            String invocationId = "CountReducer-" + UUID.randomUUID();
            nots.add(createEdge(invocationId, a.id, "used", 0));
            nots.add(createEdge(invocationId, b.id, "used", 0));
            String outputId = "CountReducer-out-" + UUID.randomUUID();

            return new WordCount(a.getWord(), a.getCount() + b.getCount(), outputId);
        }
    }

    private static final class CountEmitter implements FlatMapFunction<WordCount, String> {
        @Override
        public void flatMap(WordCount value, Collector<String> collector) throws Exception {
            collector.collect(value.toString());
        }
    }

    public static class WordCount {

        public String word;
        public long count;
        public String prov;
        public String id;

        public WordCount() {
        }

        public WordCount(String prov) {
            this.prov = prov;
        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WordCount(String word, long count, String id) {
            this.word = word;
            this.count = count;
            this.id = id;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public String toStringWithId() {
            return word + ":" + count + ":" + id;
        }

        @Override
        public String toString() {
            return word + ":" + count;
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
