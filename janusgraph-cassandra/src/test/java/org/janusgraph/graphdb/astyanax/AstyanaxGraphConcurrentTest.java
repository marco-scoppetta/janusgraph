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

package org.janusgraph.graphdb.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.TestCategory;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphConcurrentTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(TestCategory.PERFORMANCE_TESTS)
public class AstyanaxGraphConcurrentTest extends JanusGraphConcurrentTest {

    @BeforeAll
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }


    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }
}
