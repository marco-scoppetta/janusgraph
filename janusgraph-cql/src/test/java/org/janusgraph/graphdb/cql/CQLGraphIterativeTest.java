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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLStoreManagerFactory;
import org.janusgraph.diskstorage.cql.utils.CassandraStorageSetup;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.JanusGraphIterativeBenchmark;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.BeforeAll;

public class CQLGraphIterativeTest extends JanusGraphIterativeBenchmark {

    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        return CassandraStorageSetup.getCQLConfigurationWithRandomKeyspace().getConfiguration();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        BasicConfiguration basicConfiguration = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        return new CQLStoreManagerFactory(basicConfiguration).getManager(basicConfiguration);
    }


    @BeforeAll
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
