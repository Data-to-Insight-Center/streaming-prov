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

import edu.indiana.d2i.flink.utils.TestState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TestLocalReducer extends KeyedProcessFunction<String, ObjectNode, String> {

    private ValueState<TestState> state;
    private static final int LOCAL_LIMIT = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.limit"));
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", TestState.class));
        System.out.println("@@@ local limit = " + LOCAL_LIMIT);
        System.out.println("@@@ local timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("test local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(ObjectNode in, Context context,
                               Collector<String> out) throws Exception {
        TestState current = state.value();
        if (current == null) {
            current = new TestState();
            current.key = in.get("partition").asText();
        }

        if (!current.started) {
            current.startTime = System.currentTimeMillis();
            current.started = true;
        }
        current.count++;
        current.numBytes += in.toString().getBytes().length;
        state.update(current);

        if (current.count == LOCAL_LIMIT) {
            System.out.println(current.key + ": count: " + current.count);
            current.count = 0;
        }
        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        TestState current = state.value();
        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
            System.out.println(current.key + ": timer: emitting test local results...");
            long time = current.lastModified - current.startTime;
            float mb = (float) current.numBytes / 1000000;
            float throughput = (float) current.numBytes / (1000 * time);
            String result = current.key + ": timer: throughput = " + throughput + "MB/s, size = " + mb + "MB, time = " + time + "ms";
            System.out.println(result);
            out.collect(result);
        }
    }

}
