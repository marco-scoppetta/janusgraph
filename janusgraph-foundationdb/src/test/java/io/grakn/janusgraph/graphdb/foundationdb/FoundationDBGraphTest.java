package io.grakn.janusgraph.graphdb.foundationdb;

import io.grakn.janusgraph.FoundationDBContainer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBGraphTest extends JanusGraphTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public WriteConfiguration getConfigurationWithRandomKeyspace() {
        ModifiableConfiguration modifiableConfiguration = fdbContainer.getFoundationDBConfigurationWithRandomKeyspace();
        String methodName = this.testInfo.getDisplayName();
        if (methodName.equals("testConsistencyEnforcement")) {
//            IsolationLevel iso = IsolationLevel.SERIALIZABLE;
//            log.debug("Forcing isolation level {} for test method {}", iso, methodName);
//            modifiableConfiguration.set(FoundationDBStoreManager.ISOLATION_LEVEL, iso.toString());
        } else {
//            IsolationLevel iso = null;
//            if (modifiableConfiguration.has(FoundationDBStoreManager.ISOLATION_LEVEL)) {
//                iso = ConfigOption.getEnumValue(modifiableConfiguration.get(FoundationDBStoreManager.ISOLATION_LEVEL),IsolationLevel.class);
//            }
//            log.debug("Using isolation level {} (null means adapter default) for test method {}", iso, methodName);
        }
        return modifiableConfiguration.getConfiguration();
    }

    @Test
    @Disabled
    @Override
    public void testClearStorage() throws Exception {

    }

    @Test
    public void testVertexCentricQuerySmall() {
        testVertexCentricQuery(1450 /*noVertices*/);
    }

    @Test
    public void testIDBlockAllocationTimeout() throws BackendException {
        config.set("ids.authority.wait-time", Duration.of(0L, ChronoUnit.NANOS));
        config.set("ids.renew-timeout", Duration.of(1L, ChronoUnit.MILLIS));
        close();
        JanusGraphFactory.drop(graph);
        open(config);
        try {
            graph.addVertex();
            fail();
        } catch (JanusGraphException ignored) {

        }

        assertTrue(graph.isOpen());

        close(); // must be able to close cleanly

        // Must be able to reopen
        open(config);

        assertEquals(0L, (long) graph.traversal().V().count().next());
    }

    @Test
    @Disabled
    @Override
    public void testLargeJointIndexRetrieval() {
        // disabled because exceeds FDB transaction commit limit
    }

    @Test
    @Override
    public void testVertexCentricQuery() {
        // updated to not exceed FDB transaction commit limit
        testVertexCentricQuery(1000 /*noVertices*/);
    }

    @Test
    public void testSuperNode(){
        GraphTraversalSource g = graph.traversal();

        Vertex from = g.addV("from").property("id", 0).next();

        List<Long> aList = LongStream.rangeClosed(0, 1000-1).boxed()
                .collect(Collectors.toList());
        aList.forEach(t -> g.addV("to").property("id", t).as("to")
                        .addE("created").from(from).to("to").property("weight", 0.4)
                        .addV("to2").as("to2")
                        .addE("created2").from("to").to("to2").iterate());

        assertEquals(1000, g.V().has("from", "id", 0).out().out().count().next().longValue());
    }
}
