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

package edu.indiana.d2i.flink.dummy;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DummyGlobalReducer extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

    private ValueState<DummyState> state;
    private static final int TIMER_INTERVAL_MS = 2000;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-state", DummyState.class));
        System.out.println("@@@ dummy global timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("dummy global reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, String> in, Context context,
                               Collector<String> out) throws Exception {
        DummyState current = state.value();
        if (current == null) {
            current = new DummyState();
            current.startTime = System.currentTimeMillis();
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ": dummy global started..");
        }

        current.count(in.f1);
        state.update(current);

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        DummyState current = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() +
                    ": emitting dummy global results : time = " + (current.lastModified - current.startTime));
            for (String key : current.getPartitionCounts().keySet()) {
                out.collect(key + ": " + current.getPartitionCounts().get(key));
            }
        }
    }

}
