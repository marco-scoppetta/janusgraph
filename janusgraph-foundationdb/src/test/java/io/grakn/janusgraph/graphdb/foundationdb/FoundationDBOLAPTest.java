package io.grakn.janusgraph.graphdb.foundationdb;

import io.grakn.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.olap.OLAPTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class FoundationDBOLAPTest extends OLAPTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }

}
