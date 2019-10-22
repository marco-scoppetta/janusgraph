Building JanusGraph
--------------

Required:

* Java 8
* Maven 3

To build without executing tests:

```
mvn clean install -DskipTests=true
```

To build with default tests:

```
mvn clean install
```

To build with default plus TinkerPop tests:

```
mvn clean install -Dtest.skip.tp=false
```

To build with only the TinkerPop tests:

```
mvn clean install -Dtest.skip.tp=false -DskipTests=true
```

To build the distribution archive:

```
mvn clean install -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true
```
This command generates the distribution archive in `janusgraph-dist/target/janusgraph-$VERSION.zip`.
For more details information, please see [here](janusgraph-dist/README.md#building-zip-archives)

