package io.grakn.janusgraph.graphdb.database.management;

import io.grakn.janusgraph.FoundationDBContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.database.management.ManagementTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;


@Testcontainers
public class FoundationDBManagementTest extends ManagementTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        return fdbContainer.getFoundationDBGraphConfiguration();
    }
}
