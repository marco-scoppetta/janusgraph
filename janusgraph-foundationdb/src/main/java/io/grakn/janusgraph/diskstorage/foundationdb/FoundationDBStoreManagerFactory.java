package io.grakn.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManagerFactory;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

public class FoundationDBStoreManagerFactory implements StoreManagerFactory {

    public FoundationDBStoreManagerFactory(Configuration configuration) {
    }

    @Override
    public KeyColumnValueStoreManager getManager(Configuration configuration) {
        try {
            return new OrderedKeyValueStoreManagerAdapter(new FoundationDBStoreManager(configuration));
        } catch (BackendException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
