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

import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class KeyedGroup16GlobalReducer extends KeyedProcessFunction<String, Tuple2<String, List<ProvEdge>>, ProvEdge> {

    private ValueState<ProvState> state;
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("global.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-state", ProvState.class));
        System.out.println("@@@ global timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("global reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, List<ProvEdge>> in, Context context,
                               Collector<ProvEdge> out) throws Exception {
        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        if (!current.started) {
            current.startTime = System.currentTimeMillis();
            current.started = true;
        }

        current.count++;
        current.handleNewEdgeGroup(in.f1);
        for (ProvEdge edge : in.f1)
            current.numBytes += edge.toJSONString().getBytes().length;
        state.update(current);

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProvEdge> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        ProvState current = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
//            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ": emitting global results...");
//            System.out.println(current.key + ": emitting global results...");

            long time = current.lastModified - current.startTime;
            float mb = (float) current.numBytes / 1000000;
            float throughput = (float) current.numBytes / (1000 * time);
            System.out.println(current.key + ": " + getRuntimeContext().getIndexOfThisSubtask() + ": timer: edges = " + current.edgeCount + " filtered = " +
                    current.filteredEdgeCount + " throughput = " + throughput + "MB/s, Size = " + mb + "MB, time = " + time + "ms");

            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges) {
                    String source = e.getSource();
                    if (source.startsWith("task_") && source.contains("_m_"))
                        continue;
//                    if (e.toString().contains("2811"))
//                        System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ":2811 global ---> " + e.toJSONString());
                    out.collect(e);
                }
            }
        }
    }

}
