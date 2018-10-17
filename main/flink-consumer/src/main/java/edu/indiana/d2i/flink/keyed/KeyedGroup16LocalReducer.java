package edu.indiana.d2i.flink.keyed;

import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class KeyedGroup16LocalReducer extends KeyedProcessFunction<String, ObjectNode, Tuple2<String, List<ProvEdge>>> {

    private ValueState<ProvState> state;
    private static final int LOCAL_LIMIT = Integer.parseInt(KeyedProv16StreamConsumer.fileProps.getProperty("local.limit"));
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProv16StreamConsumer.fileProps.getProperty("local.timer.interval"));
    private final int globalParallelism = Integer.parseInt(KeyedProv16StreamConsumer.fileProps.getProperty("global.parallelism"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", ProvState.class));
        System.out.println("@@@ local batch size = " + LOCAL_LIMIT);
        System.out.println("@@@ local timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("grouped local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(ObjectNode in, Context context,
                               Collector<Tuple2<String, List<ProvEdge>>> out) throws Exception {
        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.get("partition").asText();
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
        current.processNotification(in);
        int bytes = in.toString().getBytes().length;
        current.numBytes += bytes;
        current.numBytesCurrentInterval += bytes;
        state.update(current);
        // emit on count, may be experiment with a time period too?
        if (current.count == LOCAL_LIMIT) {
//            System.out.println(current.key + ": count: emitting grouped local results...");
            System.out.println(prepareLog(current, "count"));
            emitGroupedState(current, out);
        }

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, List<ProvEdge>>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        ProvState current = state.value();
        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
//            System.out.println(current.key + ": timer: emitting grouped local results...");
//            long time = current.lastModified - current.startTime;
//            float mb = (float) current.numBytes / 1000000;
//            float throughput = (float) current.numBytes / (1000 * time);
            System.out.println(prepareLog(current, "timer"));
            emitGroupedState(current, out);
//            System.out.println(current.key + ": timer: edges = " + current.edgeCount + " filtered = " +
//                    current.filteredEdgeCount + " throughput = " + throughput + "MB/s, Size = " + mb + "MB, time = " + time + "ms");
        }
    }

    private void emitGroupedState(ProvState current, Collector<Tuple2<String, List<ProvEdge>>> out) {
        int n = 0;
        for (String key : current.edgesBySource.keySet()) {
            List<ProvEdge> edges = current.edgesBySource.get(key);
            if (key.startsWith("task_") && key.contains("_m_")) {
                current.filteredEdgeCount += edges.size();
                continue;
            }
            current.edgeCount += edges.size();
            int partition = (globalParallelism > 1) ? (n % globalParallelism) : 0;
            out.collect(new Tuple2<>("global" + partition, edges));
            n++;
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

//    private void emitGroupedState(ProvState current, Collector<Tuple2<String, List<ProvEdge>>> out) {
//        for (String key : current.edgesByDest.keySet()) {
//            List<ProvEdge> edges = current.edgesByDest.get(key);
////            for (ProvEdge e : edges) {
////                current.nodes.add(e.getSource());
////                current.nodes.add(e.getDestination());
////            }
//            current.edgeCount += edges.size();
//            out.collect(new Tuple2<>("global", edges));
//        }
//        current.clearState();
//    }

//    private void emitGroupedState(ProvState current, Collector<Tuple2<String, List<ProvEdge>>> out) {
//        for (String key : current.edgesByDest.keySet()) {
//            List<ProvEdge> edges = current.edgesByDest.get(key);
//            StringBuilder group = new StringBuilder();
//            group.append("{\"group\":[");
//            for (ProvEdge e : edges) {
//                current.nodes.add(e.getSource());
//                current.nodes.add(e.getDestination());
//                if (edges.size() == 1)
//                    System.out.println(current.key + ": " + e.toJSONString());
//                else
//                    group.append(e.toJSONString()).append(", ");
//            }
//            group.append("]}");
//            if (edges.size() > 1)
//                System.out.println(current.key + ": " + group.toString());
//            current.edgeCount += edges.size();
//            out.collect(new Tuple2<>("global", edges));
//        }
//        current.clearState();
//    }


}
