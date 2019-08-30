package io.grakn.janusgraph.graphdb.foundationdb;

import io.grakn.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class FoundationDBOperationCountingTest extends JanusGraphOperationCountingTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }

}
