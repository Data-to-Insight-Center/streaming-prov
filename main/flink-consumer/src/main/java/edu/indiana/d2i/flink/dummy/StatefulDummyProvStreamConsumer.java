package edu.indiana.d2i.flink.dummy;

import edu.indiana.d2i.flink.utils.ProvJSONDeserializationSchema;
import edu.indiana.d2i.flink.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StatefulDummyProvStreamConsumer {

    public static Properties fileProps;
    static {
        fileProps = Utils.loadPropertiesFromFile();
        System.out.println("@@@ kafka properties loaded: " + fileProps.getProperty("bootstrap.servers"));
    }

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", fileProps.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", "local_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                fileProps.getProperty("kafka.topic"), new ProvJSONDeserializationSchema(), properties));

        DataStream<ObjectNode> filteredStream = stream.filter((FilterFunction<ObjectNode>) value -> {
            String edgeType = value.get("edgeType").asText();
            return "wasGeneratedBy".equals(edgeType) || "used".equals(edgeType);
        });

        filteredStream
                .keyBy(n -> n.get("partition").asText())
                .process(new DummyLocalReducer())
                .keyBy(t -> t.f0)
                .process(new DummyGlobalReducer()).setParallelism(1)
                .writeAsText(fileProps.getProperty("output.file.path")).setParallelism(1);

        env.execute();
    }

    private static final class DummyLocalReducer extends KeyedProcessFunction<String, ObjectNode, Tuple2<String, String>> {

        @Override
        public void processElement(ObjectNode in, Context context, Collector<Tuple2<String, String>> out) throws Exception {
            String partition = in.get("partition").asText();
            out.collect(new Tuple2<>("dummy-global", "partition-" + partition));
        }

    }

}
