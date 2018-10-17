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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class KeyedLocalReducer extends ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ProvEdge>> {

    private ValueState<ProvState> state;
    private static final int LOCAL_LIMIT = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.limit"));
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", ProvState.class));
        System.out.println("@@@ local timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context,
                               Collector<Tuple2<String, ProvEdge>> out) throws Exception {
//        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " +
//                getRuntimeContext().getIndexOfThisSubtask() + " : " + in.f1.toString());

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.processNotification(in.f1);
        state.update(current);
        // emit on count, may be experiment with a time period too?
        if (current.count == LOCAL_LIMIT) {
            System.out.println(current.key + ": count: emitting local results...");
            emitState(current, out);
        }

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, ProvEdge>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        ProvState current = state.value();
        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
            System.out.println(current.key + ": timer: emitting local results...");
            emitState(current, out);
        }
    }

    private void emitState(ProvState current, Collector<Tuple2<String, ProvEdge>> out) {
        for (String key : current.edgesBySource.keySet()) {
            List<ProvEdge> edges = current.edgesBySource.get(key);
            for (ProvEdge e : edges) {
//                if (e.toString().contains("2811"))
//                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ":2811 local ---> " + e.toJSONString());
                out.collect(new Tuple2<>("global", e));
            }
        }
        current.clearState();
    }

    private void emitGroupedState(ProvState current, Collector<Tuple2<String, ProvEdge>> out) {
        for (String key : current.edgesBySource.keySet()) {
            List<ProvEdge> edges = current.edgesBySource.get(key);
            for (ProvEdge e : edges) {
//                if (e.toString().contains("2811"))
//                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ":2811 local ---> " + e.toJSONString());
                out.collect(new Tuple2<>("global", e));
            }
        }
        current.clearState();
    }

}
