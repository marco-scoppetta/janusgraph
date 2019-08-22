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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManagerFactory;

import java.net.InetSocketAddress;
import java.util.List;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PROTOCOL_VERSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

public class CQLStoreManagerFactory implements StoreManagerFactory {

    private final Configuration readConfiguration;
    private final CqlSession session;

    public CQLStoreManagerFactory(Configuration readConfiguration) throws PermanentBackendException {
        this.readConfiguration = readConfiguration;
        this.session = initializeSession();
    }

    @Override
    public CQLStoreManager getManager(Configuration configuration) {
        return new CQLStoreManager(session, configuration);
    }

    private CqlSession initializeSession() throws PermanentBackendException {
        List<InetSocketAddress> contactPoints;
        //TODO the following 2 variables are duplicated in DistributedStoreManager
        String[] hostnames = readConfiguration.get(STORAGE_HOSTS);
        int port = readConfiguration.has(STORAGE_PORT) ? readConfiguration.get(STORAGE_PORT) : 9042;
        try {
            contactPoints = Array.of(hostnames)
                    .map(hostName -> hostName.split(":"))
                    .map(array -> Tuple.of(array[0], array.length == 2 ? Integer.parseInt(array[1]) : port))
                    .map(tuple -> new InetSocketAddress(tuple._1, tuple._2))
                    .toJavaList();
        } catch (SecurityException | ArrayIndexOutOfBoundsException | NumberFormatException e) {
            throw new PermanentBackendException("Error initialising cluster contact points", e);
        }

        final CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(readConfiguration.get(LOCAL_DATACENTER));

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        configLoaderBuilder.withString(DefaultDriverOption.SESSION_NAME, readConfiguration.get(SESSION_NAME));

        if (readConfiguration.get(PROTOCOL_VERSION) != 0) {
            configLoaderBuilder.withInt(DefaultDriverOption.PROTOCOL_VERSION, readConfiguration.get(PROTOCOL_VERSION));
        }

        if (readConfiguration.has(AUTH_USERNAME) && readConfiguration.has(AUTH_PASSWORD)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, readConfiguration.get(AUTH_USERNAME))
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, readConfiguration.get(AUTH_PASSWORD));
        }

        if (readConfiguration.get(SSL_ENABLED)) {
            configLoaderBuilder
                    .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, readConfiguration.get(SSL_TRUSTSTORE_LOCATION))
                    .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, readConfiguration.get(SSL_TRUSTSTORE_PASSWORD));
        }

        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, readConfiguration.get(LOCAL_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, readConfiguration.get(REMOTE_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, readConfiguration.get(MAX_REQUESTS_PER_CONNECTION));

        builder.withConfigLoader(configLoaderBuilder.build());
        return builder.build();
    }
}
