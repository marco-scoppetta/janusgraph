package io.grakn.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the FoundationDB storage backend.
 * These are managed under the 'fdb' namespace in the configuration.
 *
 */
@PreInitializeConfigOptions
public interface FoundationDBConfigOptions {

    ConfigNamespace FDB_NS = new ConfigNamespace(
            GraphDatabaseConfiguration.STORAGE_NS,
            "fdb",
            "FoundationDB storage backend options");

    ConfigOption<String> DIRECTORY = new ConfigOption<>(
            FDB_NS,
            "directory",
            "The name of the JanusGraph directory in FoundationDB.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL,
            "janusgraph");

    ConfigOption<Integer> VERSION = new ConfigOption<>(
            FDB_NS,
            "version",
            "The version of the FoundationDB cluster.",
            ConfigOption.Type.LOCAL,
            610);

    ConfigOption<String> CLUSTER_FILE_PATH = new ConfigOption<>(
            FDB_NS,
            "cluster-file-path",
            "Path to the FoundationDB cluster file",
            ConfigOption.Type.LOCAL,
            "default");

    ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
            FDB_NS,
            "isolation-level",
            "Options are serializable, read_committed_no_write, read_committed_with_write",
            ConfigOption.Type.LOCAL,
            "serializable");
}
