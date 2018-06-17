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

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class PengProvState extends ProvState {

    public void handleNewEdgeGroup(List<ProvEdge> newStreamEdges) {
        List<ProvEdge> edgesToDelete = new ArrayList<>();
        List<ProvEdge> edgesToAdd = new ArrayList<>();
        for (ProvEdge newEdge : newStreamEdges) {
            String destId = newEdge.getDestination();
            if (edgesBySource.containsKey(destId)) {
                // edges with current source as the destination
                List<ProvEdge> edgesFromDest = edgesBySource.get(destId);
                for (ProvEdge e : edgesFromDest)
                    edgesToAdd.add(new ProvEdge(newEdge.getSource(), e.getDestination()));
                edgesToDelete.addAll(edgesFromDest);
            } else {
                // add only edges connected to an input data item
                if (StringUtils.countMatches(destId, "_") == 1)
                    addEdge(newEdge);
            }
        }
//        if (!edgesToAdd.isEmpty())
//            handleNewEdgeGroup(edgesToAdd);

        for (ProvEdge e : edgesToAdd)
            addEdge(e);

        for (ProvEdge e : edgesToDelete)
            deleteEdge(e);
    }



}
