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

package org.janusgraph.diskstorage.cql;

import static org.janusgraph.diskstorage.cql.utils.CassandraStorageSetup.*;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.MultiWriteKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManagerFactory;
import org.junit.jupiter.api.BeforeAll;

public class CQLMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeAll
    public static void startCassandra() {
        startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getConfig() {
        return getCQLConfiguration(getClass().getSimpleName());
    }


    @Override
    public StoreManagerFactory openStorageManagerFactory() throws BackendException {
        return new CQLStoreManagerFactory(getConfig());
    }
}
