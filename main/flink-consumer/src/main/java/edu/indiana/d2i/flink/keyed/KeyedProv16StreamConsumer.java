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

package edu.indiana.d2i.flink.keyed;

import edu.indiana.d2i.flink.utils.ProvJSONDeserializationSchema;
import edu.indiana.d2i.flink.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;

import java.util.Properties;
import java.util.UUID;

public class KeyedProv16StreamConsumer {

    public static Properties fileProps;
    static {
        fileProps = Utils.loadPropertiesFromFile();
        System.out.println("@@@ kafka properties loaded: " + fileProps.getProperty("bootstrap.servers"));
    }

    public static void main(String[] args) throws Exception {
        int localParallelism = Integer.parseInt(fileProps.getProperty("local.parallelism"));
        int globalParallelism = Integer.parseInt(fileProps.getProperty("global.parallelism"));

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(localParallelism);

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

        if ("true".equals(fileProps.getProperty("local.only")))  {
            filteredStream
                    .keyBy(n -> n.get("partition").asText())
                    .process(new KeyedGroup16LocalReducer())
                    .writeAsText(fileProps.getProperty("output.file.path"));
        } else {
            final boolean globalPeriodic = "true".equals(fileProps.getProperty("global.periodic.flush"));
            filteredStream
                    .keyBy(n -> n.get("partition").asText())
                    .process(new KeyedGroup16LocalReducer())
                    .keyBy(t -> t.f0)
                    .process(globalPeriodic ? new KeyedGroup16PeriodicGlobalReducer() : new KeyedGroup16GlobalReducer()).setParallelism(globalParallelism)
                    .writeAsText(fileProps.getProperty("output.file.path")).setParallelism(globalParallelism);
        }

        env.execute();
    }

    private static class PartitionMapper extends RichMapFunction<ObjectNode, Tuple2<String, ObjectNode>> {

        private Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("provMeter", new DropwizardMeterWrapper(meter));
        }

        @Override
        public Tuple2<String, ObjectNode> map(ObjectNode value) throws Exception {
            this.meter.markEvent();
            return new Tuple2<>(value.get("partition").asText(), value);
        }

    }

}
