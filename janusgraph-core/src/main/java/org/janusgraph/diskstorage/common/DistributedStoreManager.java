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

package org.janusgraph.diskstorage.common;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;
import java.util.Random;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Abstract class that handles configuration options shared by all distributed storage backends
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class DistributedStoreManager extends AbstractStoreManager {

    protected final TimestampProvider times;

    private static final Random random = new Random();
    protected final String[] hostnames;
    protected final int port;
    private final int pageSize;

    protected final String username;
    protected final String password;


    public DistributedStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig);
        this.hostnames = storageConfig.get(STORAGE_HOSTS);
        Preconditions.checkArgument(hostnames.length > 0, "No hostname configured");
        if (storageConfig.has(STORAGE_PORT)) this.port = storageConfig.get(STORAGE_PORT);
        else this.port = portDefault;
        this.pageSize = storageConfig.get(PAGE_SIZE);
        this.times = storageConfig.get(TIMESTAMP_PROVIDER);

        if (storageConfig.has(AUTH_USERNAME)) {
            this.username = storageConfig.get(AUTH_USERNAME);
            this.password = storageConfig.get(AUTH_PASSWORD);
        } else {
            this.username = null;
            this.password = null;
        }
    }

    /**
     * Returns a randomly chosen host name. This is used to pick one host when multiple are configured
     *
     * @return
     */
    private String getSingleHostname() {
        return hostnames[random.nextInt(hostnames.length)];
    }

    /**
     * Returns the default configured page size for this storage backend. The page size is used to determine
     * the number of records to request at a time when streaming result data.
     *
     * @return
     */
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String toString() {
        String hn = getSingleHostname();
        return hn.substring(0, Math.min(hn.length(), 256)) + ":" + port;
    }
}
