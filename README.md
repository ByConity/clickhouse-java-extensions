# ClickHouse Java extensions

* Hudi MoR Table Read
* Hive Table Read
* Las ByteLake/ManagedHive Table Read

## Build Java

```bash
cd java
mvn clean package -DskipTests
```

## Build C++

```bash
cd cpp
cmake -B build .
cmake --build build
```

Run test:

```bash
cd cpp
cmake -B build -DENABLE_TEST=ON
cmake --build build
CLASSPATH=../java/paimon/target/paimon-reader-jar-with-dependencies.jar JNI_LOG_DIR=build/jni build/clickhouse_jni_test
```
