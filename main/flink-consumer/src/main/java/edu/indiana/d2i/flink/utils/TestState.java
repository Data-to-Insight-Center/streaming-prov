package edu.indiana.d2i.flink.utils;

public class TestState {

    public String key;
    public long count;
    public long lastModified;
    public boolean started = false;
    public long numBytes;
    public long startTime;
    public long startMemory;
    public long maxMemFootprint = 0;

}
