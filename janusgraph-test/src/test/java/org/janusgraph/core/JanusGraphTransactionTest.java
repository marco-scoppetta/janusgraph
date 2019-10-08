// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core;

import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

//This test needs to be placed somewhere else once we'll start refactoring Tests
// as of now it is just used to check we don't forget to remove internal references to automagic transaction that JanusGraph starts
// whenever doing any operation on the graph like graph.addVertex()
class JanusGraphTransactionTest {

    @Test
    void whenAutomaticGraphTransactionIsClosed_removeReferenceToInternalJanusTransaction(){
        StandardJanusGraph graph = JanusGraphFactory.open("inmemory");
        graph.tx().close();
        assertFalse(graph.tx().isOpen());
        // DO NOT USE graph.getCurrentThreadTx() as this method will create a new Tx if it does not exist/it is closed
        assertNull(((StandardJanusGraph.AutomaticLocalTinkerTransaction) graph.tx()).getJanusTransaction());
        graph.close();
    }

    @Test
    void whenGraphIsClosed_closeAndRemoveReferenceToAutomaticInternalJanusTransaction(){
        StandardJanusGraph graph = JanusGraphFactory.open("inmemory");
        graph.addVertex("banana");
        assertTrue(graph.tx().isOpen());
        graph.close();
        assertFalse(graph.tx().isOpen());
        // DO NOT USE graph.getCurrentThreadTx() as this method will create a new Tx if it does not exist/it is closed
        assertNull(((StandardJanusGraph.AutomaticLocalTinkerTransaction) graph.tx()).getJanusTransaction());
    }

}