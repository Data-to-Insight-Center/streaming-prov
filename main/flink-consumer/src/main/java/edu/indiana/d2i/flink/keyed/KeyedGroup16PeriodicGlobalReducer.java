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

public class KeyedGroup16PeriodicGlobalReducer extends KeyedProcessFunction<String, Tuple2<String, List<ProvEdge>>, ProvEdge> {

    private ValueState<ProvState> state;
    private static final int GLOBAL_LIMIT = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("global.limit"));
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("global.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-state", ProvState.class));
        System.out.println("@@@ global batch size = " + GLOBAL_LIMIT);
        System.out.println("@@@ global timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("periodic global reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
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
        if (!current.startedCurrentInterval) {
            current.startTimeCurrentInterval = System.currentTimeMillis();
            current.startedCurrentInterval = true;
        }

        current.count++;
        current.handleNewEdgeGroup(in.f1);
        for (ProvEdge edge : in.f1) {
            int length = edge.toJSONString().getBytes().length;
            current.numBytes += length;
            current.numBytesCurrentInterval += length;
        }
        state.update(current);

        if (current.count == GLOBAL_LIMIT) {
//            System.out.println(current.key + ": count: emitting grouped global results...");
            System.out.println(prepareLog(current, "count"));
            emitState(current, out);
        }

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
//            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ": global: timer: emitting grouped global results...");
//            long time = current.lastModified - current.startTime;
//            float mb = (float) current.numBytes / 1000000;
//            float throughput = (float) current.numBytes / (1000 * time);
            System.out.println(prepareLog(current, "timer"));
            emitState(current, out);
//            System.out.println(current.key + ": " + getRuntimeContext().getIndexOfThisSubtask() + ": timer: edges = " + current.edgeCount + " filtered = " +
//                    current.filteredEdgeCount + " throughput = " + throughput + "MB/s, Size = " + mb + "MB, time = " + time + "ms");
        }
    }

    private void emitState(ProvState current, Collector<ProvEdge> out) throws Exception {
        for (String key : current.edgesBySource.keySet()) {
            List<ProvEdge> edges = current.edgesBySource.get(key);
            for (ProvEdge e : edges) {
                String source = e.getSource();
                if (source.startsWith("task_") && source.contains("_m_"))
                    continue;
                out.collect(e);
            }
        }
        current.clearState();
    }

    private String prepareLog(ProvState current, String who) {
        long startTime = "timer".equals(who) ? current.startTime : current.startTimeCurrentInterval;
        long time = current.lastModified - startTime;
        long bytes = "timer".equals(who) ? current.numBytes : current.numBytesCurrentInterval;
        float mb = (float) bytes / 1000000;
        float throughput = (float) bytes / (1000 * time);
        return current.key + ": " + getRuntimeContext().getIndexOfThisSubtask() + ": " + who + ": edges = " + current.edgeCount + " filtered = " +
                current.filteredEdgeCount + " throughput = " + throughput + "MB/s, Size = " + mb + "MB, time = " + time + "ms";
    }

}
