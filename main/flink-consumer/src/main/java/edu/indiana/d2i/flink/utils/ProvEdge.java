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

package edu.indiana.d2i.flink.utils;

public class ProvEdge {

    private String source;
    private String destination;

    public ProvEdge(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    @Override
    public String toString() {
        return "<" + source + ", " + destination + ">";
    }

    public boolean equals(Object obj) {
        if (obj instanceof ProvEdge) {
            ProvEdge edge = (ProvEdge) obj;
            return edge.toString().equals(this.toString());
        } else
            return false;
    }

    public String toJSONString() {
        return "{\"sourceId\":\"" + source + "\", \"destId\":\"" + destination + "\"}";
    }
}
