package edu.indiana.d2i.flink.twitter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TwitterHashCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "local_consumer");

        DataStream<String> twitterStream = env.addSource(new FlinkKafkaConsumer010<>(
                "tweets", new SimpleStringSchema(), properties));

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                twitterStream.flatMap(new Tokenizer())
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1).setParallelism(1);

        // emit result
        counts.writeAsText("file:///home/isuru/2018thesiswork/software/flink-1.6.0/output/bar").setParallelism(1);
//        counts.print();

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
    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\s+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0 && token.startsWith("#")) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
