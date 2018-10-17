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
