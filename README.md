# ybloader
Custom CSV Loader for YugaByte DB

# Build

```
mvn clean package
```

# Run
Need to specify: cassandra port (e.g. `127.0.0.1`), input file (e.g. `data.csv`), input keyspace and table names, and a comma separated list of the columns whose values are given in the input file (in that order).

```
java -jar ./target/yb-loader-1.0-SNAPSHOT-jar-with-dependencies.jar <cassandra_port> <input_file.csv> target_keyspace target_table "<col1>,<col2>,..<coln>"
```