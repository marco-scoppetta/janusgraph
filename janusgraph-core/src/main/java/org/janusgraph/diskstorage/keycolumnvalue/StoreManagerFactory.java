package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.configuration.Configuration;

public interface StoreManagerFactory {

    KeyColumnValueStoreManager getManager(Configuration configuration);
}
