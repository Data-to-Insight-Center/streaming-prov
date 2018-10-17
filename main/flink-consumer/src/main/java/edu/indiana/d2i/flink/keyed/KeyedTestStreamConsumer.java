package edu.indiana.d2i.flink.keyed;

import edu.indiana.d2i.flink.utils.ProvJSONDeserializationSchema;
import edu.indiana.d2i.flink.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class KeyedTestStreamConsumer {

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
        properties.setProperty("auto.offset.reset", "earliest");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                fileProps.getProperty("kafka.topic"), new ProvJSONDeserializationSchema(), properties));

        DataStream<ObjectNode> filteredStream = stream.filter((FilterFunction<ObjectNode>) value -> {
            String edgeType = value.get("edgeType").asText();
            return "wasGeneratedBy".equals(edgeType) || "used".equals(edgeType);
        });

        filteredStream
                .keyBy(n -> n.get("partition").asText())
                .process(new TestLocalReducer())
                .writeAsText(fileProps.getProperty("output.file.path"));

        env.execute();
    }

}
