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

package edu.indiana.d2i.flink.processor;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class ProvStreamProcessor {

//    private Map<String, List<ProvEdge>> edgesBySource = new HashMap<>();
//    private Map<String, List<ProvEdge>> edgesByDest = new HashMap<>();
//
//    public void processNotification(ObjectNode n) {
//        if (n.get("group") == null) {
//            handleNewEdge(new ProvEdge(n.get("sourceId").asText(), n.get("destId").asText()));
//        } else {
//            ArrayNode array = (ArrayNode) n.get("group");
//            Iterator<JsonNode> it = array.elements();
//            List<ProvEdge> newEdges = new ArrayList<>();
//            while (it.hasNext()) {
//                JsonNode node = it.next();
//                newEdges.add(new ProvEdge(node.get("sourceId").asText(), node.get("destId").asText()));
//            }
//            handleNewEdgeGroup(newEdges);
//        }
//    }
//
//    public void printState() {
//        for (String key : edgesBySource.keySet()) {
//            List<ProvEdge> edges = edgesBySource.get(key);
//            for (ProvEdge e : edges) {
//                System.out.println("<" + e.getSource() + ", " + e.getDestination() + ">");
//            }
//        }
//    }
//
//    private void handleNewEdge(ProvEdge newStreamEdge) {
//        List<ProvEdge> newEdges = new ArrayList<>();
//        newEdges.add(newStreamEdge);
//        handleNewEdgeGroup(newEdges);
//    }
//
//    private void handleNewEdgeGroup(List<ProvEdge> newStreamEdges) {
//        List<ProvEdge> edgesToDelete = new ArrayList<>();
//        for (ProvEdge newEdge : newStreamEdges) {
//            if (edgesByDest.containsKey(newEdge.getSource())) {
//                // edges with current source as the destination
//                List<ProvEdge> edgesIntoSource = edgesByDest.get(newEdge.getSource());
//                List<ProvEdge> newEdges = new ArrayList<>();
//                for (ProvEdge e : edgesIntoSource)
//                    newEdges.add(new ProvEdge(e.getSource(), newEdge.getDestination()));
//                edgesToDelete.addAll(edgesIntoSource);
//                handleNewEdgeGroup(newEdges);
//            } else if (edgesBySource.containsKey(newEdge.getDestination())) {
//                // edges with current source as the destination
//                List<ProvEdge> edgesFromDest = edgesBySource.get(newEdge.getDestination());
//                List<ProvEdge> newEdges = new ArrayList<>();
//                for (ProvEdge e : edgesFromDest)
//                    newEdges.add(new ProvEdge(newEdge.getSource(), e.getDestination()));
//                edgesToDelete.addAll(edgesFromDest);
//                handleNewEdgeGroup(newEdges);
//            } else {
//                addEdge(newEdge);
//            }
//        }
//
//        for (ProvEdge e : edgesToDelete)
//            deleteEdge(e);
//    }
//
//    private void addEdge(ProvEdge edge) {
//        addToMap(edgesBySource, edge, edge.getSource());
//        addToMap(edgesByDest, edge, edge.getDestination());
//    }
//
//    private void deleteEdge(ProvEdge edge) {
//        removeFromMap(edgesBySource, edge, edge.getSource());
//        removeFromMap(edgesByDest, edge, edge.getDestination());
//    }
//
//    private void addToMap(Map<String, List<ProvEdge>> edgeMap, ProvEdge edge, String key) {
//        List<ProvEdge> edgesForKey = edgeMap.get(key);
//        if (edgesForKey == null) {
//            edgesForKey = new ArrayList<>();
//            edgeMap.put(key, edgesForKey);
//        }
//        edgesForKey.add(edge);
//    }
//
//    private void removeFromMap(Map<String, List<ProvEdge>> edgeMap, ProvEdge edge, String key) {
//        List<ProvEdge> edgesForKey = edgeMap.get(key);
//        if (edgesForKey != null && edgesForKey.contains(edge)) {
//            edgesForKey.remove(edge);
//            if (edgesForKey.isEmpty())
//                edgeMap.remove(key);
//        }
//    }

}
