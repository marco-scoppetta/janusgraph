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

package org.janusgraph.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.configuration.builder.ReadConfigurationBuilder;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManagerFactory;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.log.StandardLogProcessorFramework;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_CONF_FILE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_CONF_FILE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;
import static org.janusgraph.graphdb.management.JanusGraphManager.JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG;
import static org.janusgraph.util.system.LoggerUtil.sanitizeAndLaunder;

/**
 * JanusGraphFactory is used to open or instantiate a JanusGraph graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraph
 */

public class JanusGraphFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JanusGraphFactory.class);

    /**
     * Opens a {@link JanusGraph} database.
     * <p>
     * If the argument points to a configuration file, the configuration file is loaded to configure the JanusGraph graph
     * If the string argument is a configuration short-cut, then the short-cut is parsed and used to configure the returned JanusGraph graph.
     * <p>
     * A configuration short-cut is of the form:
     * [STORAGE_BACKEND_NAME]:[DIRECTORY_OR_HOST]
     *
     * @param shortcutOrFile Configuration file name or configuration short-cut
     * @return JanusGraph graph database configured according to the provided configuration
     * @see <a href="https://docs.janusgraph.org/latest/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="https://docs.janusgraph.org/latest/config-ref.html">Configuration Reference</a>
     */
    public static StandardJanusGraph open(String shortcutOrFile) {
        return open(getLocalConfiguration(shortcutOrFile));
    }

    /**
     * Opens a {@link JanusGraph} database.
     * <p>
     * If the argument points to a configuration file, the configuration file is loaded to configure the JanusGraph graph
     * If the string argument is a configuration short-cut, then the short-cut is parsed and used to configure the returned JanusGraph graph.
     * This method shouldn't be called by end users; it is used by internal server processes to
     * open graphs defined at server start that do not include the graphname property.
     * <p>
     * A configuration short-cut is of the form:
     * [STORAGE_BACKEND_NAME]:[DIRECTORY_OR_HOST]
     *
     * @param shortcutOrFile Configuration file name or configuration short-cut
     * @param backupName     Backup name for graph
     * @return JanusGraph graph database configured according to the provided configuration
     * @see <a href="https://docs.janusgraph.org/latest/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="https://docs.janusgraph.org/latest/config-ref.html">Configuration Reference</a>
     */
    public static StandardJanusGraph open(String shortcutOrFile, String backupName) {
        return open(getLocalConfiguration(shortcutOrFile), backupName);
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     * @see <a href="https://docs.janusgraph.org/latest/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="https://docs.janusgraph.org/latest/config-ref.html">Configuration Reference</a>
     */
    public static StandardJanusGraph open(Configuration configuration) {
        return open(new CommonsConfiguration(configuration));
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static StandardJanusGraph open(BasicConfiguration configuration) {
        return open(configuration.getConfiguration());
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static StandardJanusGraph open(ReadConfiguration configuration) {
        return open(configuration, null);
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     * This method shouldn't be called by end users; it is used by internal server processes to
     * open graphs defined at server start that do not include the graphname property.
     *
     * @param configuration Configuration for the graph database
     * @param backupName    Backup name for graph
     * @return JanusGraph graph database
     */
    private static StandardJanusGraph open(ReadConfiguration configuration, String backupName) {
        // Create BasicConfiguration out of ReadConfiguration for local configuration
        BasicConfiguration localBasicConfiguration = new BasicConfiguration(ROOT_NS, configuration, BasicConfiguration.Restriction.NONE);

        // Initialise StoreManager Factory with local configuration
        StoreManagerFactory storeManagerFactory = getFactory(localBasicConfiguration);
        // Initialise Store Manager used to connect to 'system_properties' to read global configuration
        KeyColumnValueStoreManager storeManager = storeManagerFactory.getManager(localBasicConfiguration);
        // Configurations read from system_properties -> Global for every graph existing in the current DB
        ReadConfiguration globalConfig = ReadConfigurationBuilder.buildGlobalConfiguration(localBasicConfiguration, storeManager, new KCVSConfigurationBuilder());
        // Create BasicConfiguration out of ReadConfiguration for global configuration
        BasicConfiguration globalBasicConfig = new BasicConfiguration(ROOT_NS, globalConfig, BasicConfiguration.Restriction.NONE);

        // Merge and sanitise local and global configuration to get final Graph configuration which incorporates all necessary configs.
        GraphDatabaseConfiguration dbConfig = GraphDatabaseConfigurationBuilder.build(localBasicConfiguration, globalBasicConfig, storeManager);

        Backend backend = new Backend(dbConfig.getConfiguration(), storeManagerFactory);


        // When user specifies graphname property is because he wishes to register the graph with the GraphManager
        // The GraphManager though needs to be enabled using the YAML properties file
        String graphName = localBasicConfiguration.has(GRAPH_NAME) ? localBasicConfiguration.get(GRAPH_NAME) : backupName;
        JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (null != graphName) {
            Preconditions.checkNotNull(jgm, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
            return (StandardJanusGraph) jgm.openGraph(graphName, gName -> new StandardJanusGraph(dbConfig, backend));
        } else {
            // If the GraphManager is not null, but graphname is, the user probably forgot to set it, that's why the warning.
            if (jgm != null) {
                LOG.warn("You should supply \"graph.graphname\" in your .properties file configuration if you are opening " +
                        "a graph that has not already been opened at server start, i.e. it was " +
                        "defined in your YAML file. This will ensure the graph is tracked by the JanusGraphManager, " +
                        "which will enable autocommit and rollback functionality upon all gremlin script executions. " +
                        "Note that JanusGraphFactory#open(String === shortcut notation) does not support consuming the property " +
                        "\"graph.graphname\" so these graphs should be accessed dynamically by supplying a .properties file here " +
                        "or by using the ConfiguredGraphFactory.");
            }
            return new StandardJanusGraph(dbConfig, backend);
        }
    }

    /**
     * Return a Set of graph names stored in the {@link JanusGraphManager}
     *
     * @return Set&lt;String&gt;
     */
    public static Set<String> getGraphNames() {
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        Preconditions.checkNotNull(jgm, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
        return jgm.getGraphNames();
    }

    /**
     * Removes {@link Graph} from {@link JanusGraphManager} graph reference tracker, if exists
     * there.
     *
     * @param graph Graph
     */
    public static void close(Graph graph) throws Exception {
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (jgm != null) {
            jgm.removeGraph(((StandardJanusGraph) graph).getGraphName());
        }
        graph.close();
    }

    /**
     * Drop graph database, deleting all data in storage and indexing backends. Graph can be open or closed (will be
     * closed as part of the drop operation). The graph is also removed from the {@link JanusGraphManager}
     * graph reference tracker, if there.
     *
     * <p><b>WARNING: This is an irreversible operation that will delete all graph and index data.</b></p>
     *
     * @param graph JanusGraph graph database. Can be open or closed.
     * @throws BackendException If an error occurs during deletion
     */
    public static void drop(JanusGraph graph) throws BackendException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardJanusGraph, "Invalid graph instance detected: %s", graph.getClass());
        final StandardJanusGraph g = (StandardJanusGraph) graph;
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (jgm != null) {
            jgm.removeGraph(g.getGraphName());
        }
        if (graph.isOpen()) {
            graph.close();
        }
        org.janusgraph.diskstorage.configuration.Configuration backendConfiguration = g.getConfiguration().getConfiguration();
        StoreManagerFactory storeManagerFactory = getFactory(backendConfiguration);
        Backend backend = new Backend(backendConfiguration, storeManagerFactory);
        try {
            backend.clearStorage();
        } finally {
            IOUtils.closeQuietly(backend);
        }
    }

    /**
     * Returns a {@link Builder} that allows to set the configuration options for opening a JanusGraph graph database.
     * <p>
     * In the builder, the configuration options for the graph can be set individually. Once all options are configured,
     * the graph can be opened with {@link org.janusgraph.core.JanusGraphFactory.Builder#open()}.
     *
     * @return
     */
    public static Builder build() {
        return new Builder();
    }

    //--------------------- BUILDER -------------------------------------------

    public static class Builder {

        private final WriteConfiguration writeConfiguration;

        private Builder() {
            writeConfiguration = new CommonsConfiguration();
        }

        /**
         * Configures the provided configuration path to the given value.
         *
         * @param path
         * @param value
         * @return
         */
        public Builder set(String path, Object value) {
            writeConfiguration.set(path, value);
            return this;
        }

        /**
         * Opens a JanusGraph graph with the previously configured options.
         *
         * @return
         */
        public StandardJanusGraph open() {
            ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    writeConfiguration.copy(), BasicConfiguration.Restriction.NONE);
            return JanusGraphFactory.open(mc);
        }


    }

    /**
     * Returns a {@link org.janusgraph.core.log.LogProcessorFramework} for processing transaction LOG entries
     * against the provided graph instance.
     *
     * @param graph
     * @return
     */
    public static LogProcessorFramework openTransactionLog(JanusGraph graph) {
        return new StandardLogProcessorFramework((StandardJanusGraph) graph);
    }

    /**
     * Returns a {@link TransactionRecovery} process for recovering partially failed transactions. The recovery process
     * will start processing the write-ahead transaction LOG at the specified transaction time.
     *
     * @param graph
     * @param start
     * @return
     */
    public static TransactionRecovery startTransactionRecovery(JanusGraph graph, Instant start) {
        return new StandardTransactionLogProcessor((StandardJanusGraph) graph, start);
    }

    //###################################
    //          HELPER METHODS
    //###################################


    //This is temporarily public because needed in tests, to be made private once we fix tingz around
    public static StoreManagerFactory getFactory(org.janusgraph.diskstorage.configuration.Configuration configuration) {
        String className = "org.janusgraph.diskstorage.cql.CQLStoreManagerFactory";
        return ConfigurationUtil.instantiate(className, new Object[]{configuration}, new Class[]{org.janusgraph.diskstorage.configuration.Configuration.class});
    }

    private static ReadConfiguration getLocalConfiguration(String shortcutOrFile) {
        File file = new File(shortcutOrFile);
        if (file.exists()) return getLocalConfiguration(file);
        else {
            int pos = shortcutOrFile.indexOf(':');
            if (pos < 0) pos = shortcutOrFile.length();
            String backend = shortcutOrFile.substring(0, pos);
            Preconditions.checkArgument(StandardStoreManager.getAllManagerClasses().containsKey(backend.toLowerCase()), "Backend shorthand unknown: %s", backend);
            String secondArg = null;
            if (pos + 1 < shortcutOrFile.length()) secondArg = shortcutOrFile.substring(pos + 1).trim();
            BaseConfiguration config = new BaseConfiguration();
            ModifiableConfiguration writeConfig = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(config), BasicConfiguration.Restriction.NONE);
            writeConfig.set(STORAGE_BACKEND, backend);
            ConfigOption option = Backend.getOptionForShorthand(backend);
            if (option == null) {
                Preconditions.checkArgument(secondArg == null);
            } else if (option == STORAGE_DIRECTORY || option == STORAGE_CONF_FILE) {
                Preconditions.checkArgument(StringUtils.isNotBlank(secondArg), "Need to provide additional argument to initialize storage backend");
                writeConfig.set(option, getAbsolutePath(secondArg));
            } else if (option == STORAGE_HOSTS) {
                Preconditions.checkArgument(StringUtils.isNotBlank(secondArg), "Need to provide additional argument to initialize storage backend");
                writeConfig.set(option, new String[]{secondArg});
            } else throw new IllegalArgumentException("Invalid configuration option for backend " + option);
            return new CommonsConfiguration(config);
        }
    }

    /**
     * Load a properties file containing a JanusGraph graph configuration.
     * <p>
     * <ol>
     * <li>Load the file contents into a {@link org.apache.commons.configuration.PropertiesConfiguration}</li>
     * <li>For each key that points to a configuration object that is either a directory
     * or local file, check
     * whether the associated value is a non-null, non-absolute path. If so,
     * then prepend the absolute path of the parent directory of the provided configuration {@code file}.
     * This has the effect of making non-absolute backend
     * paths relative to the config file's directory rather than the JVM's
     * working directory.
     * <li>Return the {@link ReadConfiguration} for the prepared configuration file</li>
     * </ol>
     * <p>
     *
     * @param file A properties file to load
     * @return A configuration derived from {@code file}
     */
    private static ReadConfiguration getLocalConfiguration(File file) {
        Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file, but was given: %s", file.toString());

        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(file);

            final File tmpParent = file.getParentFile();
            final File configParent;

            if (null == tmpParent) {
                /*
                 * null usually means we were given a JanusGraph config file path
                 * string like "foo.properties" that refers to the current
                 * working directory of the process.
                 */
                configParent = new File(System.getProperty("user.dir"));
            } else {
                configParent = tmpParent;
            }

            Preconditions.checkNotNull(configParent);
            Preconditions.checkArgument(configParent.isDirectory());

            // TODO this mangling logic is a relic from the hardcoded string days; it should be deleted and rewritten as a setting on ConfigOption
            final Pattern p = Pattern.compile("(" +
                    Pattern.quote(STORAGE_NS.getName()) + "\\..*" +
                    "(" + Pattern.quote(STORAGE_DIRECTORY.getName()) + "|" +
                    Pattern.quote(STORAGE_CONF_FILE.getName()) + ")"
                    + "|" +
                    Pattern.quote(INDEX_NS.getName()) + "\\..*" +
                    "(" + Pattern.quote(INDEX_DIRECTORY.getName()) + "|" +
                    Pattern.quote(INDEX_CONF_FILE.getName()) + ")"
                    + ")");

            final Iterator<String> keysToMangle = Iterators.filter(configuration.getKeys(), key -> null != key && p.matcher(key).matches());

            while (keysToMangle.hasNext()) {
                String k = keysToMangle.next();
                Preconditions.checkNotNull(k);
                final String s = configuration.getString(k);
                Preconditions.checkArgument(StringUtils.isNotBlank(s), "Invalid Configuration: key %s has null empty value", k);
                configuration.setProperty(k, getAbsolutePath(configParent, s));
            }
            return new CommonsConfiguration(configuration);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + file, e);
        }
    }

    private static String getAbsolutePath(String file) {
        return getAbsolutePath(new File(System.getProperty("user.dir")), file);
    }

    private static String getAbsolutePath(final File configParent, final String file) {
        final File storeDirectory = new File(file);
        if (!storeDirectory.isAbsolute()) {
            String newFile = configParent.getAbsolutePath() + File.separator + file;
            LOG.debug("Overwrote relative path: was {}, now {}", sanitizeAndLaunder(file), sanitizeAndLaunder(newFile));
            return newFile;
        } else {
            LOG.debug("Loaded absolute path for key: {}", sanitizeAndLaunder(file));
            return file;
        }
    }

}
