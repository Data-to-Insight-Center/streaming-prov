package edu.indiana.d2i.flink.twitter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TwitterHashCountMR {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "local_consumer");

        DataStream<String> twitterStream = env.addSource(new FlinkKafkaConsumer010<>(
                "tweets", new SimpleStringSchema(), properties));

        DataStream<String> counts = twitterStream.flatMap(new Tokenizer())
                .keyBy("word")
                .reduce(new CountReducer())
                .flatMap(new CountEmitter());

        // emit result
        counts.writeAsText("file:///home/isuru/2018thesiswork/software/flink-1.6.0/output/bar").setParallelism(1);
//        counts.print();

        // execute program
        env.execute("Streaming Hash Count");
    }


    private static final class Tokenizer implements FlatMapFunction<String, TwitterHashCountMRProv.WordCount> {

        @Override
        public void flatMap(String value, Collector<TwitterHashCountMRProv.WordCount> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\s+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0 && token.startsWith("#")) {
                    out.collect(new TwitterHashCountMRProv.WordCount(token, 1L));
                }
            }
        }
    }

    private static final class CountReducer implements ReduceFunction<TwitterHashCountMRProv.WordCount> {
        @Override
        public TwitterHashCountMRProv.WordCount reduce(TwitterHashCountMRProv.WordCount a, TwitterHashCountMRProv.WordCount b) throws Exception {
            return new TwitterHashCountMRProv.WordCount(a.getWord(), a.getCount() + b.getCount());
        }
    }

    private static final class CountEmitter implements FlatMapFunction<TwitterHashCountMRProv.WordCount, String> {
        @Override
        public void flatMap(TwitterHashCountMRProv.WordCount value, Collector<String> collector) throws Exception {
            collector.collect(value.toString());
        }
    }


}
