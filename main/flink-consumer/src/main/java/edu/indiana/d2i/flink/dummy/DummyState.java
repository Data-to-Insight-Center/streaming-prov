package edu.indiana.d2i.flink.dummy;

import java.util.HashMap;
import java.util.Map;

public class DummyState {

    private Map<String, Integer> partitionCounts;
    public long lastModified;
    public long startTime;

    public DummyState() {
        this.partitionCounts = new HashMap<>();
    }

    public void count(String partition) {
        int currentCount = partitionCounts.containsKey(partition) ? partitionCounts.get(partition) : 0;
        partitionCounts.put(partition, currentCount + 1);
    }

    public Map<String, Integer> getPartitionCounts() {
        return partitionCounts;
    }

}
