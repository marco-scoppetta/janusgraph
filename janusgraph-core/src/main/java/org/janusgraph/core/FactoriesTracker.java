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
