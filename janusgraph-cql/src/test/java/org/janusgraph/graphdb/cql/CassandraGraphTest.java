// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.cql;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CassandraStorageSetup;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class CassandraGraphTest extends JanusGraphTest {

    @BeforeAll
    public static void startCassandra() {
        System.out.println("Starting cassandra...");
        CassandraStorageSetup.startCleanEmbedded();
        System.out.println("Cassandra Started!");
    }

    @Test
    public void testHasTTL() {
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void testStorageVersionSet() {
        close();
        WriteConfiguration wc = getConfiguration();
        assertNull(wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION),
                GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION.getDatatype()));
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        mgmt = graph.openManagement();
        assertEquals(JanusGraphConstants.STORAGE_VERSION, (mgmt.get("graph.storage-version")));
        mgmt.rollback();
    }

    @Test
    public void testGraphConfigUsedByThreadBoundTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "LOCAL_QUORUM");

        long s = System.currentTimeMillis();
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        long e = System.currentTimeMillis();
        System.out.println("Opening graph second time took: " + (e - s));

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.getCurrentThreadTx();
        assertEquals("ALL",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("LOCAL_QUORUM",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
    }

    @Test
    public void testGraphConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "TWO");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "THREE");

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("THREE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testCustomConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(WRITE_CONSISTENCY), "ALL");

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction()
                .customOption(ConfigElement.getPath(READ_CONSISTENCY), "ONE")
                .customOption(ConfigElement.getPath(WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testTitanGraphBackwardCompatibility() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(KEYSPACE), "titan");
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "x.x.x");

        assertNull(wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION),
                GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION.getDatatype()));

        assertFalse(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())
        ));

        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "1.0.0");
        assertTrue(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())
        ));

        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.IDS_STORE_NAME), JanusGraphConstants.TITAN_ID_STORE_NAME);
        assertTrue(JanusGraphConstants.TITAN_ID_STORE_NAME.equals(
                wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.IDS_STORE_NAME),
                        GraphDatabaseConfiguration.IDS_STORE_NAME.getDatatype())
        ));
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
    }
}
