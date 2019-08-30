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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.util.system.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_FACTOR;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.USE_EXTERNAL_LOCKING;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.EXCEPTION_MAPPER;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

/**
 * This class creates see {@link CQLKeyColumnValueStore CQLKeyColumnValueStores} and handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 */
public class CQLStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreManager.class);

    static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    static final String CONSISTENCY_QUORUM = "QUORUM";

    private static final int DEFAULT_PORT = 9042;

    private final String keyspace;
    private final int batchSize;
    private final boolean atomicBatch;

    final ExecutorService executorService;

    @Resource
    private CqlSession session;
    private final StoreFeatures storeFeatures;
    private final Map<String, CQLKeyColumnValueStore> openStores;
    private final Deployment deployment;

    /**
     * Constructor for the {@link CQLStoreManager} given a JanusGraph {@link Configuration}.
     *
     * @param configuration
     */
    public CQLStoreManager(CqlSession session, Configuration configuration) {
        super(configuration, DEFAULT_PORT);
        this.keyspace = determineKeyspaceName(configuration);
        this.batchSize = configuration.get(BATCH_STATEMENT_SIZE);
        this.atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);

        this.executorService = new ThreadPoolExecutor(10,
                100,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("CQLStoreManager[%02d]")
                        .build());

        this.session = session;
        initializeKeyspace();

        Configuration global = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        Configuration local = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        Boolean onlyUseLocalConsistency = configuration.get(ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS);

        Boolean useExternalLocking = configuration.get(USE_EXTERNAL_LOCKING);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        fb.batchMutation(true).distributed(true);
        fb.timestamps(true).cellTTL(true);
        fb.keyConsistent((onlyUseLocalConsistency ? local : global), local);
        fb.locking(useExternalLocking);
        fb.optimisticLocking(true);
        fb.multiQuery(false);

        String partitioner = this.session.getMetadata().getTokenMap().get().getPartitionerName();
        switch (partitioner.substring(partitioner.lastIndexOf('.') + 1)) {
            case "RandomPartitioner":
            case "Murmur3Partitioner": {
                fb.keyOrdered(false).orderedScan(false).unorderedScan(true);
                deployment = Deployment.REMOTE;
                break;
            }
            case "ByteOrderedPartitioner": {
                fb.keyOrdered(true).orderedScan(true).unorderedScan(false);
                deployment = (hostnames.length == 1)// mark deployment as local only in case we have byte ordered partitioner and local
                        // connection
                        ? (NetworkUtil.isLocalConnection(hostnames[0])) ? Deployment.LOCAL : Deployment.REMOTE
                        : Deployment.REMOTE;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
            }
        }
        this.storeFeatures = fb.build();
        this.openStores = new ConcurrentHashMap<>();
    }

    void initializeKeyspace() {
        // if the keyspace already exists, just return
        if (this.session.getMetadata().getKeyspace(this.keyspace).isPresent()) {
            return;
        }

        final Configuration configuration = getStorageConfig();

        // Setting replication strategy based on value reading from the configuration: either "SimpleStrategy" or "NetworkTopologyStrategy"
        final Map<String, Object> replication = Match(configuration.get(REPLICATION_STRATEGY)).of(
                Case($("SimpleStrategy"), strategy -> HashMap.<String, Object>of("class", strategy, "replication_factor", configuration.get(REPLICATION_FACTOR))),
                Case($("NetworkTopologyStrategy"),
                        strategy -> HashMap.<String, Object>of("class", strategy)
                                .merge(Array.of(configuration.get(REPLICATION_OPTIONS))
                                        .grouped(2)
                                        .toMap(array -> Tuple.of(array.get(0), Integer.parseInt(array.get(1)))))))
                .toJavaMap();

        session.execute(createKeyspace(this.keyspace)
                .ifNotExists()
                .withReplicationOptions(replication)
                .build());
    }

    ExecutorService getExecutorService() {
        return this.executorService;
    }

    CqlSession getSession() {
        return this.session;
    }

    String getKeyspaceName() {
        return this.keyspace;
    }

    @VisibleForTesting
    Map<String, String> getCompressionOptions(final String name) throws BackendException {
        KeyspaceMetadata keyspaceMetadata1 = this.session.getMetadata().getKeyspace(this.keyspace)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));

        TableMetadata tableMetadata = keyspaceMetadata1.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));

        Object compressionOptions = tableMetadata.getOptions().get(CqlIdentifier.fromCql("compression"));

        return (Map<String, String>) compressionOptions;
    }

    @VisibleForTesting
    TableMetadata getTableMetadata(final String name) throws BackendException {
        final KeyspaceMetadata keyspaceMetadata = (this.session.getMetadata().getKeyspace(this.keyspace))
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));
        return keyspaceMetadata.getTable(name)
                .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));
    }

    @Override
    public void close() {
        this.executorService.shutdownNow();
    }

    @Override
    public String getName() {
        return String.format("%s.%s", getClass().getSimpleName(), this.keyspace);
    }

    @Override
    public Deployment getDeployment() {
        return this.deployment;
    }

    @Override
    public StoreFeatures getFeatures() {
        return this.storeFeatures;
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name, final Container metaData) throws BackendException {
        return this.openStores.computeIfAbsent(name, n -> new CQLKeyColumnValueStore(this, n, getStorageConfig(), () -> this.openStores.remove(n)));
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return new CQLTransaction(config);
    }

    @Override
    public void clearStorage() throws BackendException {
        if (this.storageConfig.get(DROP_ON_CLEAR)) {
            this.session.execute(dropKeyspace(this.keyspace).build());
        } else if (this.exists()) {
            final Future<Seq<AsyncResultSet>> result = Future.sequence(
                    Iterator.ofAll(this.session.getMetadata().getKeyspace(this.keyspace).get().getTables().values())
                            .map(table -> Future.fromJavaFuture(this.session.executeAsync(truncate(this.keyspace, table.getName().toString()).build())
                                    .toCompletableFuture())));
            result.await();
        } else {
            LOGGER.info("Keyspace {} does not exist in the cluster", this.keyspace);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return session.getMetadata().getKeyspace(this.keyspace).isPresent();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutateMany(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        if (this.atomicBatch) {
            mutateManyLogged(mutations, txh);
        } else {
            mutateManyUnlogged(mutations, txh);
        }
    }

    // Use a single logged batch
    private void mutateManyLogged(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        builder.setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel());
        builder.addStatements(Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            final String tableName = tableNameAndMutations.getKey();
            final Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            final CQLKeyColumnValueStore columnValueStore = Option.of(this.openStores.get(tableName))
                    .getOrElseThrow(() -> new IllegalStateException("Store cannot be found: " + tableName));
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                final StaticBuffer key = keyAndMutations.getKey();
                final KCVMutation keyMutations = keyAndMutations.getValue();

                final Iterator<BatchableStatement<BoundStatement>> deletions = Iterator.of(commitTime.getDeletionTime(this.times))
                        .flatMap(deleteTime -> Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deleteTime)));
                final Iterator<BatchableStatement<BoundStatement>> additions = Iterator.of(commitTime.getAdditionTime(this.times))
                        .flatMap(addTime -> Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, addTime)));

                return Iterator.concat(deletions, additions);
            });
        }));
        final Future<AsyncResultSet> result = Future.fromJavaFuture(this.executorService, this.session.executeAsync(builder.build()).toCompletableFuture());

        result.await();
        if (result.isFailure()) {
            throw EXCEPTION_MAPPER.apply(result.getCause().get());
        }
        sleepAfterWrite(txh, commitTime);
    }

    // Create an async un-logged batch per partition key
    private void mutateManyUnlogged(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        final Future<Seq<AsyncResultSet>> result = Future.sequence(this.executorService, Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            final String tableName = tableNameAndMutations.getKey();
            final Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            final CQLKeyColumnValueStore columnValueStore = Option.of(this.openStores.get(tableName))
                    .getOrElseThrow(() -> new IllegalStateException("Store cannot be found: " + tableName));
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                final StaticBuffer key = keyAndMutations.getKey();
                final KCVMutation keyMutations = keyAndMutations.getValue();

                final Iterator<BatchableStatement<BoundStatement>> deletions = Iterator.of(commitTime.getDeletionTime(this.times))
                        .flatMap(deleteTime -> Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deleteTime)));
                final Iterator<BatchableStatement<BoundStatement>> additions = Iterator.of(commitTime.getAdditionTime(this.times))
                        .flatMap(addTime -> Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, addTime)));

                return Iterator.concat(deletions, additions)
                        .grouped(this.batchSize)
                        .map(group -> Future.fromJavaFuture(this.executorService,
                                this.session.executeAsync(
                                        BatchStatement.newInstance(DefaultBatchType.LOGGED)
                                                .addAll(group)
                                                .setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel()))
                                        .toCompletableFuture()));
            });
        }));

        result.await();
        if (result.isFailure()) {
            throw EXCEPTION_MAPPER.apply(result.getCause().get());
        }
        sleepAfterWrite(txh, commitTime);
    }

    private String determineKeyspaceName(Configuration config) {
        if ((!config.has(KEYSPACE) && (config.has(GRAPH_NAME)))) return config.get(GRAPH_NAME);
        return config.get(KEYSPACE);
    }
}
