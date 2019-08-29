package org.janusgraph.core;

import java.util.HashSet;
import java.util.Set;

public class FactoriesTracker {

    static private Set<String> factories = new HashSet<>();


    static public void addFactory(String hashCode) {
        boolean add = factories.add(hashCode);
        if (!add) {
            System.out.println("TRYING TO ADD TWICE " + hashCode);
        }
    }

    static public void removeFactory(String hashCode) {
        boolean remove = factories.remove(hashCode);
        if (!remove) {
            System.out.println("FAILED TO REMOVE " + hashCode);
        }
    }

    static public int openFactories() {
        return factories.size();
    }
}
