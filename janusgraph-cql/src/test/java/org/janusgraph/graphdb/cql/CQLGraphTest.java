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

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.junit.jupiter.api.Assertions.*;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.utils.CassandraStorageSetup;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

public class CQLGraphTest extends CassandraGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCQLConfiguration(getClass().getSimpleName()).getConfiguration();
    }
}
