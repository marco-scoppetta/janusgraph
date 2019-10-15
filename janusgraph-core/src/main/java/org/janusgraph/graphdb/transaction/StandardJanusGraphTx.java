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

package org.janusgraph.graphdb.transaction;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.ReadOnlyTransactionException;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.SchemaInspector;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.Hex;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;
import org.janusgraph.graphdb.database.serialize.AttributeHandler;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.MetricsQueryExecutor;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.QueryExecutor;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.ConditionUtil;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.graph.IndexQueryBuilder;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.MultiVertexCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.VertexCentricQuery;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationComparator;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.tinkerpop.ElementUtils;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;
import org.janusgraph.graphdb.transaction.addedrelations.ConcurrentBufferAddedRelations;
import org.janusgraph.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import org.janusgraph.graphdb.transaction.indexcache.ConcurrentIndexCache;
import org.janusgraph.graphdb.transaction.indexcache.IndexCache;
import org.janusgraph.graphdb.transaction.indexcache.SimpleIndexCache;
import org.janusgraph.graphdb.transaction.vertexcache.VertexCache;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.janusgraph.graphdb.types.StandardPropertyKeyMaker;
import org.janusgraph.graphdb.types.StandardVertexLabelMaker;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.TypeUtil;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.types.system.SystemRelationType;
import org.janusgraph.graphdb.types.system.SystemTypeManager;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
import org.janusgraph.graphdb.util.SubQueryIterator;
import org.janusgraph.graphdb.util.VertexCentricEdgeIterable;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.janusgraph.util.datastructures.Retriever;
import org.janusgraph.util.stats.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JanusGraphTransaction defines a transactional context for a {@link JanusGraph}. Since JanusGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a JanusGraphTransaction.
 * <p>
 * All vertex and edge retrievals are channeled by a graph transaction which bundles all such retrievals, creations and
 * deletions into one transaction. A graph transaction is analogous to a
 * <a href="https://en.wikipedia.org/wiki/Database_transaction">database transaction</a>.
 * The isolation level and <a href="https://en.wikipedia.org/wiki/ACID">ACID support</a> are configured through the storage
 * backend, meaning whatever level of isolation is supported by the storage backend is mirrored by a graph transaction.
 * <p>
 * A graph transaction supports:
 * <ul>
 * <li>Creating vertices, properties and edges</li>
 * <li>Creating types</li>
 * <li>Index-based retrieval of vertices</li>
 * <li>Querying edges and vertices</li>
 * <li>Aborting and committing transaction</li>
 * </ul>
 */

public class StandardJanusGraphTx implements JanusGraphTransaction, TypeInspector, SchemaInspector {

    private static final Logger LOG = LoggerFactory.getLogger(StandardJanusGraphTx.class);

    private static final Map<Long, InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();

    /**
     * This is a workaround for #893.  Cache sizes small relative to the level
     * of thread parallelism can lead to JanusGraph generating multiple copies of
     * a single vertex in a single transaction.
     */
    private static final long MIN_VERTEX_CACHE_SIZE = 100L;


    private final StandardJanusGraph graph;
    private final TransactionConfiguration config;
    private final IDManager idManager;
    private final AttributeHandler attributeHandler;
    private final BackendTransaction backendTransaction;
    private final EdgeSerializer edgeSerializer;
    private final IndexSerializer indexSerializer;

    /* ###############################################
            Internal Data Structures
     ############################################### */

    //####### Vertex Cache
    /**
     * Keeps track of vertices already loaded in memory. Cannot release vertices with added relations.
     */
    private final VertexCache vertexCache;

    //######## Data structures that keep track of new and deleted elements
    //These data structures cannot release elements, since we would loose track of what was added or deleted
    /**
     * Keeps track of all added relations in this transaction
     */
    private final AddedRelationsContainer addedRelations;
    /**
     * Keeps track of all deleted relations in this transaction
     */
    private volatile Map<Long, InternalRelation> deletedRelations;

    //######## Index Caches
    /**
     * Caches the result of index calls so that repeated index queries don't need
     * to be passed to the IndexProvider. This cache will drop entries when it overflows
     * since the result set can always be retrieved from the IndexProvider
     */
    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;
    /**
     * Builds an inverted index for newly added properties so they can be considered in index queries.
     * This cache my not release elements since that would entail an expensive linear scan over addedRelations
     */
    private final IndexCache newVertexIndexEntries;

    //####### Other Data structures
    /**
     * Caches JanusGraph types by name so that they can be quickly retrieved once they are loaded in the transaction.
     * Since type retrieval by name is common and there are only a few types, since cache is a simple map (i.e. no release)
     */
    private final Map<String, Long> newTypeCache;

    /**
     * Used to assign temporary ids to new vertices and relations added in this transaction.
     * If ids are assigned immediately, this is not used. This IDPool is shared across all elements.
     */
    private final IDPool temporaryIds;

    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider timestampProvider;

    /**
     * Whether or not this transaction is open
     */
    private volatile boolean isOpen;

    private final VertexConstructor existingVertexRetriever;
    private final VertexConstructor internalVertexRetriever;

    public StandardJanusGraphTx(StandardJanusGraph graph, TransactionConfiguration config) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(config);
        this.graph = graph;
        this.timestampProvider = graph.getConfiguration().getTimestampProvider();
        this.config = config;
        this.idManager = graph.getIDManager();
        this.attributeHandler = graph.getDataSerializer();
        this.edgeSerializer = graph.getEdgeSerializer();
        this.indexSerializer = graph.getIndexSerializer();
        this.temporaryIds = buildTemporaryIDsPool();
        this.isOpen = true;

        this.internalVertexRetriever = new VertexConstructor(config.hasVerifyInternalVertexExistence());
        this.existingVertexRetriever = new VertexConstructor(false);


        int concurrencyLevel = (config.isSingleThreaded()) ? 1 : 4;
        this.addedRelations = (config.isSingleThreaded()) ? new SimpleBufferAddedRelations() : new ConcurrentBufferAddedRelations();
        this.newTypeCache = (config.isSingleThreaded()) ? new HashMap<>() : new NonBlockingHashMap<>();
        this.newVertexIndexEntries = (config.isSingleThreaded()) ? new SimpleIndexCache() : new ConcurrentIndexCache();


        long effectiveVertexCacheSize = Math.max(MIN_VERTEX_CACHE_SIZE, config.getVertexCacheSize()); // this is because of a weird bug with cache, see line 119
        this.vertexCache = new VertexCache(effectiveVertexCacheSize, concurrencyLevel, config.getDirtyVertexSize());
        this.indexCache = CacheBuilder.newBuilder().weigher((Weigher<JointIndexQuery.Subquery, List<Object>>) (q, r) -> 2 + r.size()).concurrencyLevel(concurrencyLevel).maximumWeight(config.getIndexCacheWeight()).build();

        this.deletedRelations = EMPTY_DELETED_RELATIONS;

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "begin").inc();
            elementProcessor = new MetricsQueryExecutor<>(config.getGroupName(), "graph", elementProcessorImpl);
            edgeProcessor = new MetricsQueryExecutor<>(config.getGroupName(), "vertex", edgeProcessorImpl);
        } else {
            elementProcessor = elementProcessorImpl;
            edgeProcessor = edgeProcessorImpl;
        }

        this.backendTransaction = graph.openBackendTransaction(this); // awkward!
    }

    private IDPool buildTemporaryIDsPool() {
        return new IDPool() {

            private final AtomicLong counter = new AtomicLong(1);

            @Override
            public long nextID() {
                return counter.getAndIncrement();
            }

            @Override
            public void close() {
                //Do nothing
            }
        };
    }


    @Override
    public Features features() {
        return getGraph().features();
    }

    @Override
    public Variables variables() {
        return getGraph().variables();
    }

    @Override
    public Configuration configuration() {
        return getGraph().configuration();
    }

    @Override
    public <I extends Io> I io(Io.Builder<I> builder) {
        return getGraph().io(builder);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        StandardJanusGraph graph = getGraph();
        if (isOpen()) commit();
        return graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null; // TODO think about this at some point in the future.
    }

    /**
     * Creates a new vertex in the graph with the given vertex id.
     * Note, that an exception is thrown if the vertex id is not a valid JanusGraph vertex id or if a vertex with the given
     * id already exists. Only accepts long ids - all others are ignored.
     * <p>
     * A valid JanusGraph vertex ids must be provided. Use {@link org.janusgraph.graphdb.idmanagement.IDManager#toVertexId(long)}
     * to construct a valid JanusGraph vertex id from a user id, where <code>idManager</code> can be obtained through
     * {@link org.janusgraph.graphdb.database.StandardJanusGraph#getIDManager()}.
     * <pre>
     * <code>long vertexId = ((StandardJanusGraph) graph).getIDManager().toVertexId(userVertexId);</code>
     * </pre>
     *
     * @param keyValues key-value pairs of properties to characterize or attach to the vertex
     * @return New vertex
     */
    @Override
    public JanusGraphVertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i + 1];
                Preconditions.checkArgument(labelValue instanceof VertexLabel || labelValue instanceof String,
                        "Expected a string or VertexLabel as the vertex label argument, but received: %s", labelValue);
                if (labelValue instanceof String) ElementHelper.validateLabel((String) labelValue);
            }
        }
        VertexLabel label = BaseVertexLabel.DEFAULT_VERTEXLABEL;
        if (labelValue != null) {
            label = (labelValue instanceof VertexLabel) ? (VertexLabel) labelValue : getOrCreateVertexLabel((String) labelValue);
        }

        Long id = ElementHelper.getIdValue(keyValues).map(Number.class::cast).map(Number::longValue).orElse(null);
        JanusGraphVertex vertex = addVertex(id, label);
        org.janusgraph.graphdb.util.ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds == null || vertexIds.length == 0) return (Iterator) getVertices().iterator();
        ElementUtils.verifyArgsMustBeEitherIdOrElement(vertexIds);
        long[] ids = new long[vertexIds.length];
        int pos = 0;
        for (Object vertexId : vertexIds) {
            ids[pos++] = ElementUtils.getVertexId(vertexId);
        }
        return (Iterator) getVertices(ids).iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds == null || edgeIds.length == 0) return (Iterator) getEdges().iterator();
        ElementUtils.verifyArgsMustBeEitherIdOrElement(edgeIds);
        RelationIdentifier[] ids = new RelationIdentifier[edgeIds.length];
        int pos = 0;
        for (Object edgeId : edgeIds) {
            ids[pos++] = ElementUtils.getEdgeId(edgeId);
        }
        return (Iterator) getEdges(ids).iterator();
    }

    @Override
    public String toString() {
        int ihc = System.identityHashCode(this);
        String ihcString = String.format("0x%s", Hex.bytesToHex(
                (byte) (ihc >>> 24 & 0x000000FF),
                (byte) (ihc >>> 16 & 0x000000FF),
                (byte) (ihc >>> 8 & 0x000000FF),
                (byte) (ihc & 0x000000FF)));
        return StringFactory.graphString(this, ihcString);
    }

    @Override
    public Transaction tx() {
        return new AbstractThreadedTransaction(getGraph()) {
            @Override
            public void doOpen() {
                if (isClosed()) throw new IllegalStateException("Cannot re-open a closed transaction.");
            }

            @Override
            public void doCommit() {
                StandardJanusGraphTx.this.commit();
            }

            @Override
            public void doRollback() {
                StandardJanusGraphTx.this.rollback();
            }

            @Override
            public <G extends Graph> G createThreadedTx() {
                throw new UnsupportedOperationException("JanusGraph does not support nested transactions.");
            }

            @Override
            public boolean isOpen() {
                return StandardJanusGraphTx.this.isOpen();
            }

            @Override
            protected void doClose() {
                if (isOpen()) {
                    throw Exceptions.openTransactionsOnClose();
                }
                super.doClose();
            }
        };
    }

    @Override
    public void close() {
        tx().close();
    }

    /*
     * ------------------------------------ Utility Access Verification methods ------------------------------------
     */

    private void verifyWriteAccess(JanusGraphVertex... vertices) {
        if (config.isReadOnly()) {
            throw new ReadOnlyTransactionException("Cannot create new entities in read-only transaction");
        }
        for (JanusGraphVertex v : vertices) {
            if (v.hasId() && idManager.isUnmodifiableVertex(v.longId()) && !v.isNew()) {
                throw new SchemaViolationException("Cannot modify unmodifiable vertex: " + v);
            }
        }
        verifyAccess(vertices);
    }

    private void verifyAccess(JanusGraphVertex... vertices) {
        verifyOpen();
        for (JanusGraphVertex v : vertices) {
            Preconditions.checkArgument(v instanceof InternalVertex, "Invalid vertex: %s", v);
            if (!(v instanceof SystemRelationType) && this != ((InternalVertex) v).tx()) {
                throw new IllegalStateException("The vertex or type is not associated with this transaction [" + v + "]");
            }
            if (v.isRemoved()) {
                throw new IllegalStateException("The vertex or type has been removed [" + v + "]");
            }
        }
    }

    private void verifyOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Operation cannot be executed because the enclosing transaction is closed");
        }
    }

    /*
     * ------------------------------------ External Access ------------------------------------
     */

    public StandardJanusGraphTx getNextTx() {
        Preconditions.checkArgument(isClosed());
        if (!config.isThreadBound()) {
            throw new IllegalStateException("Cannot access element because its enclosing transaction is closed and unbound");
        } else {
            return (StandardJanusGraphTx) graph.getCurrentThreadTx();
        }
    }

    public TransactionConfiguration getConfiguration() {
        return config;
    }

    public StandardJanusGraph getGraph() {
        return graph;
    }

    public BackendTransaction getBackendTransaction() {
        return backendTransaction;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public IDManager getIdManager() {
        return idManager;
    }

    public boolean isPartitionedVertex(JanusGraphVertex vertex) {
        return vertex.hasId() && idManager.isPartitionedVertex(vertex.longId());
    }

    public InternalVertex getCanonicalVertex(InternalVertex partitionedVertex) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long canonicalId = idManager.getCanonicalVertexId(partitionedVertex.longId());
        if (canonicalId == partitionedVertex.longId()) return partitionedVertex;
        else return getExistingVertex(canonicalId);
    }

    public InternalVertex getOtherPartitionVertex(JanusGraphVertex partitionedVertex, long otherPartition) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        return getExistingVertex(idManager.getPartitionedVertexId(partitionedVertex.longId(), otherPartition));
    }

    public InternalVertex[] getAllRepresentatives(JanusGraphVertex partitionedVertex, boolean restrict2Partitions) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long[] ids;
        if (!restrict2Partitions || !config.hasRestrictedPartitions()) {
            ids = idManager.getPartitionedVertexRepresentatives(partitionedVertex.longId());
        } else {
            int[] restrictedPartitions = config.getRestrictedPartitions();
            ids = new long[restrictedPartitions.length];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = idManager.getPartitionedVertexId(partitionedVertex.longId(), restrictedPartitions[i]);
            }
        }
        Preconditions.checkArgument(ids.length > 0);
        InternalVertex[] vertices = new InternalVertex[ids.length];
        for (int i = 0; i < ids.length; i++) vertices[i] = getExistingVertex(ids[i]);
        return vertices;
    }


    /*
     * ------------------------------------ Vertex Handling ------------------------------------
     */

    private boolean containsVertex(long vertexId) {
        return getVertex(vertexId) != null;
    }

    private boolean isValidVertexId(long id) {
        return id > 0 && (idManager.isSchemaVertexId(id) || idManager.isUserVertexId(id));
    }

    @Override
    public JanusGraphVertex getVertex(long vertexId) {
        verifyOpen();
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVertexByID").inc();
        }
        if (!isValidVertexId(vertexId)) return null;
        //Make canonical partitioned vertex id
        if (idManager.isPartitionedVertex(vertexId)) vertexId = idManager.getCanonicalVertexId(vertexId);

        InternalVertex v = vertexCache.get(vertexId, internalVertexRetriever);
        return (null == v || v.isRemoved()) ? null : v;
    }

    @Override
    public Iterable<JanusGraphVertex> getVertices(long... ids) {
        verifyOpen();
        if (ids == null || ids.length == 0) return (Iterable) getInternalVertices();

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVerticesByID").inc();
        }
        List<JanusGraphVertex> result = new ArrayList<>(ids.length);
        LongArrayList vertexIds = new LongArrayList(ids.length);

        for (long id : ids) {
            if (isValidVertexId(id)) {
                if (idManager.isPartitionedVertex(id)) id = idManager.getCanonicalVertexId(id);
                if (vertexCache.contains(id)) {
                    result.add(vertexCache.get(id, existingVertexRetriever));
                } else {
                    vertexIds.add(id);
                }
            }
        }

        if (!vertexIds.isEmpty()) {
            if (internalVertexRetriever.hasVerifyExistence()) {
                List<EntryList> existence = graph.edgeMultiQuery(vertexIds, graph.vertexExistenceQuery, backendTransaction);
                for (int i = 0; i < vertexIds.size(); i++) {
                    if (!existence.get(i).isEmpty()) {
                        long id = vertexIds.get(i);
                        result.add(vertexCache.get(id, existingVertexRetriever));
                    }
                }
            } else {
                for (int i = 0; i < vertexIds.size(); i++) {
                    result.add(vertexCache.get(vertexIds.get(i), internalVertexRetriever));
                }
            }
        }
        //Filter out potentially removed vertices
        result.removeIf(JanusGraphElement::isRemoved);
        return result;
    }

    private InternalVertex getExistingVertex(long vertexId) {
        //return vertex no matter what, even if deleted, and assume the id has the correct format
        return vertexCache.get(vertexId, existingVertexRetriever);
    }

    public InternalVertex getInternalVertex(long vertexId) {
        //return vertex but potentially check for existence
        return vertexCache.get(vertexId, internalVertexRetriever);
    }

    private class VertexConstructor implements Retriever<Long, InternalVertex> {

        private final boolean verifyExistence;

        private VertexConstructor(boolean verifyExistence) {
            this.verifyExistence = verifyExistence;
        }

        boolean hasVerifyExistence() {
            return verifyExistence;
        }

        @Override
        public InternalVertex get(Long vertexId) {
            Preconditions.checkArgument(vertexId != null && vertexId > 0, "Invalid vertex id: %s", vertexId);
            Preconditions.checkArgument(idManager.isSchemaVertexId(vertexId) || idManager.isUserVertexId(vertexId), "Not a valid vertex id: %s", vertexId);

            byte lifecycle = ElementLifeCycle.Loaded;
            long canonicalVertexId = idManager.isPartitionedVertex(vertexId) ? idManager.getCanonicalVertexId(vertexId) : vertexId;
            if (verifyExistence) {
                if (graph.edgeQuery(canonicalVertexId, graph.vertexExistenceQuery, backendTransaction).isEmpty()) {
                    lifecycle = ElementLifeCycle.Removed;
                }
            }
            if (canonicalVertexId != vertexId) {
                //Take lifecycle from canonical representative
                lifecycle = getExistingVertex(canonicalVertexId).getLifeCycle();
            }

            InternalVertex vertex;
            if (idManager.isRelationTypeId(vertexId)) {
                if (idManager.isPropertyKeyId(vertexId)) {
                    if (IDManager.isSystemRelationTypeId(vertexId)) {
                        vertex = SystemTypeManager.getSystemType(vertexId);
                    } else {
                        vertex = new PropertyKeyVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
                    }
                } else {
                    if (IDManager.isSystemRelationTypeId(vertexId)) {
                        vertex = SystemTypeManager.getSystemType(vertexId);
                    } else {
                        vertex = new EdgeLabelVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
                    }
                }
            } else if (idManager.isVertexLabelVertexId(vertexId)) {
                vertex = new VertexLabelVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
            } else if (idManager.isGenericSchemaVertexId(vertexId)) {
                vertex = new JanusGraphSchemaVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
            } else if (idManager.isUserVertexId(vertexId)) {
                vertex = new CacheVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
            } else throw new IllegalArgumentException("ID could not be recognised");
            return vertex;
        }
    }

    @Override
    public JanusGraphVertex addVertex(Long vertexId, VertexLabel label) {
        verifyWriteAccess();
        if (label == null) label = BaseVertexLabel.DEFAULT_VERTEXLABEL;
        Preconditions.checkArgument(vertexId == null || IDManager.VertexIDType.NormalVertex.is(vertexId), "Not a valid vertex id: %s", vertexId);
        Preconditions.checkArgument(vertexId == null || ((InternalVertexLabel) label).hasDefaultConfiguration(), "Cannot only use default vertex labels: %s", label);
        Preconditions.checkArgument(vertexId == null || !config.hasVerifyExternalVertexExistence() || !containsVertex(vertexId), "Vertex with given id already exists: %s", vertexId);
        StandardVertex vertex = new StandardVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
        if (vertexId != null) {
            vertex.setId(vertexId);
        } else if (config.hasAssignIDsImmediately() || label.isPartitioned()) {
            graph.assignID(vertex, label);
        }
        addProperty(vertex, BaseKey.VertexExists, Boolean.TRUE);
        if (label != BaseVertexLabel.DEFAULT_VERTEXLABEL) { //Add label
            Preconditions.checkArgument(label instanceof VertexLabelVertex);
            addEdge(vertex, label, BaseLabel.VertexLabelEdge);
        }
        vertexCache.add(vertex);
        return vertex;
    }

    @Override
    public JanusGraphVertex addVertex(String vertexLabel) {
        return addVertex(getOrCreateVertexLabel(vertexLabel));
    }

    public JanusGraphVertex addVertex(VertexLabel vertexLabel) {
        return addVertex(null, vertexLabel);
    }

    private Iterable<InternalVertex> getInternalVertices() {
        Iterable<InternalVertex> allVertices;
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<InternalVertex> newVs = vertexCache.getAllNew();
            newVs.removeIf(internalVertex -> internalVertex instanceof JanusGraphSchemaElement);
            allVertices = Iterables.concat(newVs, new VertexIterable(graph, this));
        } else {
            allVertices = new VertexIterable(graph, this);
        }
        //Filter out all but one PartitionVertex representative
        return Iterables.filter(allVertices, internalVertex -> !isPartitionedVertex(internalVertex) || internalVertex.longId() == idManager.getCanonicalVertexId(internalVertex.longId()));
    }


    /*
     * ------------------------------------ Adding and Removing Relations ------------------------------------
     */

    public final boolean validDataType(Class datatype) {
        return attributeHandler.validDataType(datatype);
    }

    public final Object verifyAttribute(PropertyKey key, Object attribute) {
        if (attribute == null) throw new SchemaViolationException("Property value cannot be null");
        Class<?> datatype = key.dataType();
        if (datatype.equals(Object.class)) {
            if (!attributeHandler.validDataType(attribute.getClass())) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(attribute);
            }
            return attribute;
        } else {
            if (!attribute.getClass().equals(datatype)) {
                Object converted = null;
                try {
                    converted = attributeHandler.convert(datatype, attribute);
                } catch (IllegalArgumentException e) {
                    //Just means that data could not be converted
                }
                if (converted == null) {
                    throw new SchemaViolationException("Value [%s] is not an instance of the expected data type for property key [%s] and cannot be converted. Expected: %s, found: %s", attribute, key.name(), datatype, attribute.getClass());
                }
                attribute = converted;
            }
            attributeHandler.verifyAttribute(datatype, attribute);
            return attribute;
        }
    }

    public void removeRelation(InternalRelation relation) {
        Preconditions.checkArgument(!relation.isRemoved());
        relation = relation.it();
        for (int i = 0; i < relation.getLen(); i++) {
            verifyWriteAccess(relation.getVertex(i));
        }

        //Delete from Vertex
        for (int i = 0; i < relation.getLen(); i++) {
            relation.getVertex(i).removeRelation(relation);
        }
        //Update transaction data structures
        if (relation.isNew()) {
            addedRelations.remove(relation);
            if (TypeUtil.hasSimpleInternalVertexKeyIndex(relation))
                newVertexIndexEntries.remove((JanusGraphVertexProperty) relation);
        } else {
            Preconditions.checkArgument(relation.isLoaded());
            Map<Long, InternalRelation> result = deletedRelations;
            if (result == EMPTY_DELETED_RELATIONS) {
                if (config.isSingleThreaded()) {
                    deletedRelations = result = new HashMap<>();
                } else {
                    synchronized (this) {
                        result = deletedRelations;
                        if (result == EMPTY_DELETED_RELATIONS)
                            deletedRelations = result = new ConcurrentHashMap<>();
                    }
                }
            }
            result.put(relation.longId(), relation);
        }
    }

    public boolean isRemovedRelation(Long relationId) {
        return deletedRelations.containsKey(relationId);
    }

    public JanusGraphEdge addEdge(JanusGraphVertex outVertex, JanusGraphVertex inVertex, EdgeLabel label) {
        verifyWriteAccess(outVertex, inVertex);
        outVertex = ((InternalVertex) outVertex).it();
        inVertex = ((InternalVertex) inVertex).it();

        StandardEdge edge = new StandardEdge(IDManager.getTemporaryRelationID(temporaryIds.nextID()), label, (InternalVertex) outVertex, (InternalVertex) inVertex, ElementLifeCycle.New);
        if (config.hasAssignIDsImmediately()) graph.assignID(edge);
        connectRelation(edge);
        return edge;
    }

    private void connectRelation(InternalRelation r) {
        for (int i = 0; i < r.getLen(); i++) {
            boolean success = r.getVertex(i).addRelation(r);
            if (!success) throw new AssertionError("Could not connect relation: " + r);
        }
        addedRelations.add(r);
        for (int pos = 0; pos < r.getLen(); pos++) {
            vertexCache.add(r.getVertex(pos));
        }
        if (TypeUtil.hasSimpleInternalVertexKeyIndex(r)) newVertexIndexEntries.add((JanusGraphVertexProperty) r);
    }

    public JanusGraphVertexProperty addProperty(JanusGraphVertex vertex, PropertyKey key, Object value) {
        verifyWriteAccess(vertex);
        Preconditions.checkArgument(!(key instanceof ImplicitKey), "Cannot create a property of implicit type: %s", key.name());
        vertex = ((InternalVertex) vertex).it();
        Preconditions.checkNotNull(key);
        Object normalizedValue = verifyAttribute(key, value);

        StandardVertexProperty prop = new StandardVertexProperty(IDManager.getTemporaryRelationID(temporaryIds.nextID()), key, (InternalVertex) vertex, normalizedValue, ElementLifeCycle.New);
        if (config.hasAssignIDsImmediately()) {
            graph.assignID(prop);
        }
        connectRelation(prop);
        return prop;
    }

    @Override
    public Iterable<JanusGraphEdge> getEdges(RelationIdentifier... ids) {
        verifyOpen();
        if (ids == null || ids.length == 0) {
            return new VertexCentricEdgeIterable(getInternalVertices(), RelationCategory.EDGE);
        }

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getEdgesByID").inc();
        }
        List<JanusGraphEdge> result = new ArrayList<>(ids.length);
        for (RelationIdentifier id : ids) {
            if (id == null) continue;
            JanusGraphEdge edge = id.findEdge(this);
            if (edge != null && !edge.isRemoved()) result.add(edge);
        }
        return result;
    }


    /*
     * ------------------------------------ Schema Handling ------------------------------------
     */

    public final JanusGraphSchemaVertex makeSchemaVertex(JanusGraphSchemaCategory schemaCategory, String name, TypeDefinitionMap definition) {
        verifyOpen();
        Preconditions.checkArgument(!schemaCategory.hasName() || StringUtils.isNotBlank(name), "Need to provide a valid name for type [%s]", schemaCategory);
        schemaCategory.verifyValidDefinition(definition);
        JanusGraphSchemaVertex schemaVertex;
        if (schemaCategory.isRelationType()) {
            if (schemaCategory == JanusGraphSchemaCategory.PROPERTYKEY) {
                schemaVertex = new PropertyKeyVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserPropertyKey, temporaryIds.nextID()), ElementLifeCycle.New);
            } else {
                assert schemaCategory == JanusGraphSchemaCategory.EDGELABEL;
                schemaVertex = new EdgeLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel, temporaryIds.nextID()), ElementLifeCycle.New);
            }
        } else if (schemaCategory == JanusGraphSchemaCategory.VERTEXLABEL) {
            schemaVertex = new VertexLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType, temporaryIds.nextID()), ElementLifeCycle.New);
        } else {
            schemaVertex = new JanusGraphSchemaVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType, temporaryIds.nextID()), ElementLifeCycle.New);
        }

        graph.assignID(schemaVertex, BaseVertexLabel.DEFAULT_VERTEXLABEL);
        Preconditions.checkArgument(schemaVertex.longId() > 0);
        if (schemaCategory.hasName()) addProperty(schemaVertex, BaseKey.SchemaName, schemaCategory.getSchemaName(name));
        addProperty(schemaVertex, BaseKey.VertexExists, Boolean.TRUE);
        addProperty(schemaVertex, BaseKey.SchemaCategory, schemaCategory);
        updateSchemaVertex(schemaVertex);
        addProperty(schemaVertex, BaseKey.SchemaUpdateTime, timestampProvider.getTime(timestampProvider.getTime()));
        for (Map.Entry<TypeDefinitionCategory, Object> def : definition.entrySet()) {
            JanusGraphVertexProperty p = addProperty(schemaVertex, BaseKey.SchemaDefinitionProperty, def.getValue());
            p.property(BaseKey.SchemaDefinitionDesc.name(), TypeDefinitionDescription.of(def.getKey()));
        }
        vertexCache.add(schemaVertex);
        if (schemaCategory.hasName()) {
            newTypeCache.put(schemaCategory.getSchemaName(name), schemaVertex.longId());
        }
        return schemaVertex;

    }

    public void updateSchemaVertex(JanusGraphSchemaVertex schemaVertex) {
        addProperty(schemaVertex, BaseKey.SchemaUpdateTime, timestampProvider.getTime(timestampProvider.getTime()));
    }

    public PropertyKey makePropertyKey(String name, TypeDefinitionMap definition) {
        return (PropertyKey) makeSchemaVertex(JanusGraphSchemaCategory.PROPERTYKEY, name, definition);
    }

    public EdgeLabel makeEdgeLabel(String name, TypeDefinitionMap definition) {
        return (EdgeLabel) makeSchemaVertex(JanusGraphSchemaCategory.EDGELABEL, name, definition);
    }


    public JanusGraphEdge addSchemaEdge(JanusGraphVertex out, JanusGraphVertex in, TypeDefinitionCategory def, Object modifier) {
        JanusGraphEdge edge = addEdge(out, in, BaseLabel.SchemaDefinitionEdge);
        TypeDefinitionDescription desc = new TypeDefinitionDescription(def, modifier);
        edge.property(BaseKey.SchemaDefinitionDesc.name(), desc);
        return edge;
    }

    @Override
    public VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys) {
        for (PropertyKey key : keys) {
            addSchemaEdge(vertexLabel, key, TypeDefinitionCategory.PROPERTY_KEY_EDGE, null);
        }
        return vertexLabel;
    }

    @Override
    public EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys) {
        for (PropertyKey key : keys) {
            if (key.cardinality() != Cardinality.SINGLE) {
                throw new IllegalArgumentException(String.format("An Edge [%s] can not have a property [%s] with the cardinality [%s].", edgeLabel, key, key.cardinality()));
            }
            addSchemaEdge(edgeLabel, key, TypeDefinitionCategory.PROPERTY_KEY_EDGE, null);
        }
        return edgeLabel;
    }

    @Override
    public EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel) {
        addSchemaEdge(outVLabel, inVLabel, TypeDefinitionCategory.CONNECTION_EDGE, edgeLabel.name());
        addSchemaEdge(edgeLabel, outVLabel, TypeDefinitionCategory.UPDATE_CONNECTION_EDGE, null);
        return edgeLabel;
    }

    public JanusGraphSchemaVertex getSchemaVertex(String schemaName) {
        Long schemaId = newTypeCache.get(schemaName);
        if (schemaId == null) schemaId = graph.getSchemaCache().getSchemaId(schemaName);
        if (schemaId != null) {
            InternalVertex typeVertex = vertexCache.get(schemaId, existingVertexRetriever);
            return (JanusGraphSchemaVertex) typeVertex;
        } else return null;
    }

    @Override
    public boolean containsRelationType(String name) {
        return getRelationType(name) != null;
    }

    @Override
    public RelationType getRelationType(String name) {
        verifyOpen();

        RelationType type = SystemTypeManager.getSystemType(name);
        if (type != null) return type;

        return (RelationType) getSchemaVertex(JanusGraphSchemaCategory.getRelationTypeName(name));
    }

    @Override
    public boolean containsPropertyKey(String name) {
        RelationType type = getRelationType(name);
        return type != null && type.isPropertyKey();
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        RelationType type = getRelationType(name);
        return type != null && type.isEdgeLabel();
    }

    @Override
    public RelationType getExistingRelationType(long typeId) {
        if (IDManager.isSystemRelationTypeId(typeId)) {
            return SystemTypeManager.getSystemType(typeId);
        } else {
            InternalVertex v = getInternalVertex(typeId);
            return (RelationType) v;
        }
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        RelationType pk = getRelationType(name);
        Preconditions.checkArgument(pk == null || pk.isPropertyKey(), "The relation type with name [%s] is not a property key", name);
        return (PropertyKey) pk;
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name, Object value) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makePropertyKey(makePropertyKey(name), value);
        } else if (et.isPropertyKey()) {
            return (PropertyKey) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a key: " + name);
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makePropertyKey(makePropertyKey(name));
        } else if (et.isPropertyKey()) {
            return (PropertyKey) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a key: " + name);
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        RelationType el = getRelationType(name);
        Preconditions.checkArgument(el == null || el.isEdgeLabel(), "The relation type with name [%s] is not an edge label", name);
        return (EdgeLabel) el;
    }

    @Override
    public EdgeLabel getOrCreateEdgeLabel(String name) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makeEdgeLabel(makeEdgeLabel(name));
        } else if (et.isEdgeLabel()) {
            return (EdgeLabel) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a label: " + name);
    }

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return new StandardPropertyKeyMaker(this, name, indexSerializer, attributeHandler);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return new StandardEdgeLabelMaker(this, name, indexSerializer, attributeHandler);
    }

    //-------- Vertex Labels -----------------

    @Override
    public VertexLabel getExistingVertexLabel(long id) {
        assert idManager.isVertexLabelVertexId(id);
        InternalVertex v = getInternalVertex(id);
        return (VertexLabelVertex) v;
    }

    @Override
    public boolean containsVertexLabel(String name) {
        verifyOpen();
        return BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name) || getSchemaVertex(JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName(name)) != null;
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        verifyOpen();
        if (BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name)) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        return (VertexLabel) getSchemaVertex(JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName(name));
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        VertexLabel vertexLabel = getVertexLabel(name);
        if (vertexLabel == null) {
            vertexLabel = config.getAutoSchemaMaker().makeVertexLabel(makeVertexLabel(name));
        }
        return vertexLabel;
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        StandardVertexLabelMaker maker = new StandardVertexLabelMaker(this);
        maker.name(name);
        return maker;
    }
    /*
     * ------------------------------------ Query Answering ------------------------------------
     */

    public VertexCentricQueryBuilder query(JanusGraphVertex vertex) {
        return new VertexCentricQueryBuilder(((InternalVertex) vertex).it());
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(JanusGraphVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        for (JanusGraphVertex v : vertices) builder.addVertex(v);
        return builder;
    }

    public void executeMultiQuery(Collection<InternalVertex> vertices, SliceQuery sq, QueryProfiler profiler) {
        LongArrayList vertexIds = new LongArrayList(vertices.size());
        for (InternalVertex v : vertices) {
            if (!v.isNew() && v.hasId() && (v instanceof CacheVertex) && !v.hasLoadedRelations(sq))
                vertexIds.add(v.longId());
        }

        if (!vertexIds.isEmpty()) {
            List<EntryList> results = QueryProfiler.profile(profiler, sq, true, q -> graph.edgeMultiQuery(vertexIds, q, backendTransaction));
            int pos = 0;
            for (JanusGraphVertex v : vertices) {
                if (pos < vertexIds.size() && vertexIds.get(pos) == v.longId()) {
                    EntryList vresults = results.get(pos);
                    ((CacheVertex) v).loadRelations(sq, query -> vresults);
                    pos++;
                }
            }
        }
    }

    public final QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery> edgeProcessor;

    private final QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery> edgeProcessorImpl = new QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery>() {
        @Override
        public Iterator<JanusGraphRelation> getNew(VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew() || vertex.hasAddedRelations()) {
                return (Iterator) vertex.getAddedRelations(new Predicate<InternalRelation>() {
                    //Need to filter out self-loops if query only asks for one direction

                    private JanusGraphRelation previous = null;

                    @Override
                    public boolean apply(InternalRelation relation) {
                        if ((relation instanceof JanusGraphEdge) && relation.isLoop()
                                && query.getDirection() != Direction.BOTH) {
                            if (relation.equals(previous))
                                return false;

                            previous = relation;
                        }

                        return query.matches(relation);
                    }
                }).iterator();
            } else {
                return Collections.emptyIterator();
            }
        }

        @Override
        public boolean hasDeletions(VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew()) return false;
            //In addition to deleted, we need to also check for added relations since those can potentially
            //replace existing ones due to a multiplicity constraint
            return vertex.hasRemovedRelations() || vertex.hasAddedRelations();
        }

        @Override
        public boolean isDeleted(VertexCentricQuery query, JanusGraphRelation result) {
            if (deletedRelations.containsKey(result.longId()) || result != ((InternalRelation) result).it()) {
                return true;
            }
            //Check if this relation is replaced by an added one due to a multiplicity constraint
            InternalRelationType type = (InternalRelationType) result.getType();
            InternalVertex vertex = query.getVertex();
            if (type.multiplicity().isConstrained() && vertex.hasAddedRelations()) {
                RelationComparator comparator = new RelationComparator(vertex);
                return !Iterables.isEmpty(vertex.getAddedRelations(internalRelation -> comparator.compare((InternalRelation) result, internalRelation) == 0));
            }
            return false;
        }

        @Override
        public Iterator<JanusGraphRelation> execute(VertexCentricQuery query, SliceQuery sq, Object exeInfo, QueryProfiler profiler) {
            assert exeInfo == null;
            if (query.getVertex().isNew()) {
                return Collections.emptyIterator();
            }

            InternalVertex v = query.getVertex();

            EntryList iterable = v.loadRelations(sq, query1 -> QueryProfiler.profile(profiler, query1, q -> graph.edgeQuery(v.longId(), q, backendTransaction)));

            return RelationConstructor.readRelation(v, iterable, StandardJanusGraphTx.this).iterator();
        }
    };

    public final QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery> elementProcessor;

    private final QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery> elementProcessorImpl = new QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery>() {

        private PredicateCondition<PropertyKey, JanusGraphElement> getEqualityCondition(Condition<JanusGraphElement> condition) {
            if (condition instanceof PredicateCondition) {
                final PredicateCondition<PropertyKey, JanusGraphElement> pc = (PredicateCondition) condition;
                if (pc.getPredicate() == Cmp.EQUAL && TypeUtil.hasSimpleInternalVertexKeyIndex(pc.getKey())) return pc;
            } else if (condition instanceof And) {
                for (Condition<JanusGraphElement> child : condition.getChildren()) {
                    PredicateCondition<PropertyKey, JanusGraphElement> p = getEqualityCondition(child);
                    if (p != null) return p;
                }
            }
            return null;
        }


        @Override
        public Iterator<JanusGraphElement> getNew(GraphCentricQuery query) {
            //If the query is unconstrained then we don't need to add new elements, so will be picked up by getVertices()/getEdges() below
            if (query.numSubQueries() == 1 && query.getSubQuery(0).getBackendQuery().isEmpty())
                return Collections.emptyIterator();
            Preconditions.checkArgument(query.getCondition().hasChildren(), "If the query is non-empty it needs to have a condition");

            if (query.getResultType() == ElementCategory.VERTEX && hasModifications()) {
                Preconditions.checkArgument(QueryUtil.isQueryNormalForm(query.getCondition()));
                PredicateCondition<PropertyKey, JanusGraphElement> standardIndexKey = getEqualityCondition(query.getCondition());
                Iterator<JanusGraphVertex> vertices;
                if (standardIndexKey == null) {
                    Set<PropertyKey> keys = Sets.newHashSet();
                    ConditionUtil.traversal(query.getCondition(), cond -> {
                        Preconditions.checkArgument(cond.getType() != Condition.Type.LITERAL || cond instanceof PredicateCondition);
                        if (cond instanceof PredicateCondition) {
                            keys.add(((PredicateCondition<PropertyKey, JanusGraphElement>) cond).getKey());
                        }
                        return true;
                    });
                    Preconditions.checkArgument(!keys.isEmpty(), "Invalid query condition: %s", query.getCondition());
                    Set<JanusGraphVertex> vertexSet = Sets.newHashSet();
                    for (JanusGraphRelation r : addedRelations.getView(relation -> keys.contains(relation.getType()))) {
                        vertexSet.add(((JanusGraphVertexProperty) r).element());
                    }
                    for (JanusGraphRelation r : deletedRelations.values()) {
                        if (keys.contains(r.getType())) {
                            JanusGraphVertex v = ((JanusGraphVertexProperty) r).element();
                            if (!v.isRemoved()) vertexSet.add(v);
                        }
                    }
                    vertices = vertexSet.iterator();
                } else {
                    vertices = com.google.common.collect.Iterators.transform(newVertexIndexEntries.get(standardIndexKey.getValue(), standardIndexKey.getKey()).iterator(), new com.google.common.base.Function<JanusGraphVertexProperty, JanusGraphVertex>() {
                        @Nullable
                        @Override
                        public JanusGraphVertex apply(JanusGraphVertexProperty o) {
                            return o.element();
                        }
                    });
                }

                return (Iterator) com.google.common.collect.Iterators.filter(vertices, query::matches);
            } else if ((query.getResultType() == ElementCategory.EDGE || query.getResultType() == ElementCategory.PROPERTY) && !addedRelations.isEmpty()) {
                return (Iterator) addedRelations.getView(relation -> query.getResultType().isInstance(relation) && !relation.isInvisible() && query.matches(relation)).iterator();
            } else return Collections.emptyIterator();
        }


        @Override
        public boolean hasDeletions(GraphCentricQuery query) {
            return hasModifications();
        }

        @Override
        public boolean isDeleted(GraphCentricQuery query, JanusGraphElement result) {
            if (result == null || result.isRemoved()) return true;
            else if (query.getResultType() == ElementCategory.VERTEX) {
                Preconditions.checkArgument(result instanceof InternalVertex);
                InternalVertex v = ((InternalVertex) result).it();
                return (v.hasAddedRelations() || v.hasRemovedRelations()) && !query.matches(result);
            } else if (query.getResultType() == ElementCategory.EDGE || query.getResultType() == ElementCategory.PROPERTY) {
                Preconditions.checkArgument(result.isLoaded() || result.isNew());
                //Loaded relations are immutable so we don't need to check those
                //New relations could be modified in this transaction to now longer match the query, hence we need to
                //check for this case and consider the relations deleted
                return result.isNew() && !query.matches(result);
            } else throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
        }

        @Override
        public Iterator<JanusGraphElement> execute(GraphCentricQuery query, JointIndexQuery indexQuery, Object exeInfo, QueryProfiler profiler) {
            Iterator<JanusGraphElement> iterator;
            if (!indexQuery.isEmpty()) {
                List<QueryUtil.IndexCall<Object>> retrievals = new ArrayList<>();
                // Leave first index for streaming, and prepare the rest for intersecting and lookup
                for (int i = 1; i < indexQuery.size(); i++) {
                    JointIndexQuery.Subquery subquery = indexQuery.getQuery(i);
                    retrievals.add(limit -> {
                        JointIndexQuery.Subquery adjustedQuery = subquery.updateLimit(limit);
                        try {
                            return indexCache.get(adjustedQuery, () -> QueryProfiler.profile(subquery.getProfiler(), adjustedQuery, q -> indexSerializer.query(q, backendTransaction).collect(Collectors.toList())));
                        } catch (Exception e) {
                            throw new JanusGraphException("Could not call index", e.getCause());
                        }
                    });
                }
                // Constructs an iterator which lazily streams results from 1st index, and filters by looking up in the intersection of results from all other indices (if any)
                // NOTE NO_LIMIT is passed to processIntersectingRetrievals to prevent incomplete intersections, which could lead to missed results
                iterator = new SubQueryIterator(indexQuery.getQuery(0), indexSerializer, backendTransaction, indexCache, indexQuery.getLimit(), getConversionFunction(query.getResultType()),
                        retrievals.isEmpty() ? null : QueryUtil.processIntersectingRetrievals(retrievals, Query.NO_LIMIT));
            } else {
                if (config.hasForceIndexUsage()) {
                    throw new JanusGraphException("Could not find a suitable index to answer graph query and graph scans are disabled: " + query);
                }
                LOG.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());

                QueryProfiler sub = profiler.addNested("scan");
                sub.setAnnotation(QueryProfiler.QUERY_ANNOTATION, indexQuery);
                sub.setAnnotation(QueryProfiler.FULLSCAN_ANNOTATION, true);
                sub.setAnnotation(QueryProfiler.CONDITION_ANNOTATION, query.getResultType());

                switch (query.getResultType()) {
                    case VERTEX:
                        return (Iterator) getVertices().iterator();
                    case EDGE:
                        return (Iterator) getEdges().iterator();
                    case PROPERTY:
                        return new VertexCentricEdgeIterable(getInternalVertices(), RelationCategory.PROPERTY).iterator();
                    default:
                        throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
                }
            }

            return iterator;
        }

    };

    public Function<Object, ? extends JanusGraphElement> getConversionFunction(ElementCategory elementCategory) {
        switch (elementCategory) {
            case VERTEX:
                return id -> {
                    Preconditions.checkNotNull(id);
                    Preconditions.checkArgument(id instanceof Long);
                    return getInternalVertex((Long) id);
                };
            case EDGE:
                return id -> {
                    Preconditions.checkNotNull(id);
                    Preconditions.checkArgument(id instanceof RelationIdentifier);
                    return ((RelationIdentifier) id).findEdge(StandardJanusGraphTx.this);
                };
            case PROPERTY:
                return id -> {
                    Preconditions.checkNotNull(id);
                    Preconditions.checkArgument(id instanceof RelationIdentifier);
                    return ((RelationIdentifier) id).findProperty(StandardJanusGraphTx.this);
                };
            default:
                throw new IllegalArgumentException("Unexpected result type: " + elementCategory);
        }
    }

    @Override
    public GraphCentricQueryBuilder query() {
        return new GraphCentricQueryBuilder(this, graph.getIndexSerializer());
    }

    @Override
    public JanusGraphIndexQuery indexQuery(String indexName, String query) {
        return new IndexQueryBuilder(this, indexSerializer).setIndex(indexName).setQuery(query);
    }

    /*
     * ------------------------------------ Transaction State ------------------------------------
     */

    @Override
    public synchronized void commit() {
        Preconditions.checkArgument(isOpen(), "The transaction has already been closed");
        boolean success = false;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "commit").inc();
        }
        try {
            if (hasModifications()) {
                graph.commit(addedRelations.getAll(), deletedRelations.values(), this);
            } else {
                backendTransaction.commit();
            }
            success = true;
        } catch (Exception e) {
            try {
                backendTransaction.rollback();
            } catch (BackendException e1) {
                throw new JanusGraphException("Could not rollback after a failed commit", e);
            }
            throw new JanusGraphException("Could not commit transaction due to exception during persistence", e);
        } finally {
            releaseTransaction();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "commit.exceptions").inc();
            }
        }
    }

    @Override
    public synchronized void rollback() {
        Preconditions.checkArgument(isOpen(), "The transaction has already been closed");
        boolean success = false;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback").inc();
        }
        try {
            backendTransaction.rollback();
            success = true;
        } catch (Exception e) {
            throw new JanusGraphException("Could not rollback transaction due to exception", e);
        } finally {
            releaseTransaction();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback.exceptions").inc();
            }
        }
    }

    private void releaseTransaction() {
        isOpen = false;
        graph.closeTransaction(this);
    }

    @Override
    public final boolean isOpen() {
        return isOpen;
    }

    @Override
    public final boolean isClosed() {
        return !isOpen;
    }

    @Override
    public boolean hasModifications() {
        return !addedRelations.isEmpty() || !deletedRelations.isEmpty();
    }

}
