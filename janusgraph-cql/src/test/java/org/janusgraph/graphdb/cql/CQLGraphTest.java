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
import org.janusgraph.diskstorage.cql.utils.CassandraStorageSetup;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class CQLGraphTest extends JanusGraphTest {
    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        return CassandraStorageSetup.getCQLConfigurationWithRandomKeyspace().getConfiguration();
    }

    @BeforeAll
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testHasTTL() {
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void testStorageVersionSet() {
        close();
        assertNull(config.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION),
                GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION.getDatatype()));
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION);
        graph = JanusGraphFactory.open(config);
        mgmt = graph.openManagement();
        assertEquals(JanusGraphConstants.STORAGE_VERSION, (mgmt.get("graph.storage-version")));
        mgmt.rollback();
    }

    @Test
    public void testGraphConfigUsedByThreadBoundTx() {
        close();
        config.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        config.set(ConfigElement.getPath(WRITE_CONSISTENCY), "LOCAL_QUORUM");
        graph = JanusGraphFactory.open(config);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.getCurrentThreadTx();
        assertEquals("ALL",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("LOCAL_QUORUM",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
    }

    @Test
    public void testGraphConfigUsedByTx() {
        close();
        config.set(ConfigElement.getPath(READ_CONSISTENCY), "TWO");
        config.set(ConfigElement.getPath(WRITE_CONSISTENCY), "THREE");

        graph = JanusGraphFactory.open(config);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
        assertEquals("TWO",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("THREE",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testCustomConfigUsedByTx() {
        close();
        config.set(ConfigElement.getPath(READ_CONSISTENCY), "ALL");
        config.set(ConfigElement.getPath(WRITE_CONSISTENCY), "ALL");

        graph = JanusGraphFactory.open(config);

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction()
                .customOption(ConfigElement.getPath(READ_CONSISTENCY), "ONE")
                .customOption(ConfigElement.getPath(WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(READ_CONSISTENCY));
        assertEquals("TWO",
                tx.getBackendTransaction().getBaseTransactionConfig().getCustomOptions()
                        .get(WRITE_CONSISTENCY));
        tx.rollback();
    }
}
