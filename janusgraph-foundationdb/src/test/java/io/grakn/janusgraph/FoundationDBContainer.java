package io.grakn.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.Assert;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {
    private boolean firstTime = true;

    public FoundationDBContainer() {
        super("foundationdb/foundationdb:6.1.12");
        withExposedPorts(4500);
        withFixedExposedPort(4500, 4500);
        withFileSystemBind("./etc", "/etc/foundationdb");
        withEnv("FDB_NETWORKING_MODE", "host");
        waitingFor(
                Wait.forLogMessage(".*FDBD joined cluster.*\\n", 1)
        );
    }

    public ModifiableConfiguration getFoundationDBConfiguration() {
        if (firstTime) {
            initialise();
            firstTime = false;
        }
        return getFoundationDBConfiguration("janusgraph-test-fdb");
    }

    public ModifiableConfiguration getFoundationDBConfigurationWithRandomKeyspace() {
        if (firstTime) {
            initialise();
            firstTime = false;
        }
        return getFoundationDBConfiguration(UUID.randomUUID().toString());
    }

    private void initialise(){
        try {
            ExecResult fdbcli = execInContainer("fdbcli", "--exec", "configure new single ssd");
            int exitCode = fdbcli.getExitCode();
            if (exitCode != 0)
                throw new RuntimeException("Failed executing first try connection");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ModifiableConfiguration getFoundationDBConfiguration(String graphName) {
        String s = "docker:docker@127.0.0.1:4500";// + getMappedPort(4500);
        File clusterFile = null;
        try {
            clusterFile = File.createTempFile("fdb", "cluster");

            FileWriter fr = new FileWriter(clusterFile);
            fr.write(s);
            fr.close();
        } catch (IOException e) {
            Assert.fail();
        }
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND, "foundationdb")
                .set(DIRECTORY, graphName)
                .set(DROP_ON_CLEAR, false)
                .set(CLUSTER_FILE_PATH, clusterFile.getAbsolutePath())
                .set(ISOLATION_LEVEL, "read_committed_with_write");
    }

    public WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb").getConfiguration();
    }
}