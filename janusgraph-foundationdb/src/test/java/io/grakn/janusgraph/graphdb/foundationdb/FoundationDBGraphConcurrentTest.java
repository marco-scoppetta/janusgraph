package io.grakn.janusgraph.graphdb.foundationdb;

import io.grakn.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphConcurrentTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBGraphConcurrentTest extends JanusGraphConcurrentTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }

}
