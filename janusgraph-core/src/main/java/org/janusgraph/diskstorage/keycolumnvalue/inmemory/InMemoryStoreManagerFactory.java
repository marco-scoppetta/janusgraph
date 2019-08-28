package org.janusgraph.diskstorage.keycolumnvalue.inmemory;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManagerFactory;

public class InMemoryStoreManagerFactory implements StoreManagerFactory {
    public InMemoryStoreManagerFactory(Configuration configuration) {
    }

    @Override
    public KeyColumnValueStoreManager getManager(Configuration configuration) {
        return new InMemoryStoreManager(configuration);
    }

    @Override
    public void close() {

    }
}
