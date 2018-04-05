package com.yugabyte.ybloader;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class YbLoader {

  private static final Option INPUT_FILE_PATTERN = new Option("i", "input-files", true, "Input file pattern (regex based). Example: input.*.csv");
  private static final Option TABLE_NAME = new Option("t", "table", true, "Destination table name.");
  private static final Option KEYSPACE_NAME = new Option("k", "keyspace", true, "Destination keyspace name.");
  private static final Option HOST = new Option("h", "host", true, "Yugabyte server host.");
  private static final Option COLUMNS = new Option("c", "columns", true, "The list of columns in a comma delimted format. It must match the CSV file format.");
  private static final Option PARALLELISM = new Option("p", "parallelism", true, "The parallelism of loading. By default set to 1.");
  private static final Option BATCH_SIZE = new Option("b", "batch", true, "The batch size. By default set to 150.");

  private static final Options ALL_OPTIONS = new Options();

  static {
    INPUT_FILE_PATTERN.setRequired(true);
    TABLE_NAME.setRequired(true);
    KEYSPACE_NAME.setRequired(true);
    HOST.setRequired(true);
    COLUMNS.setRequired(true);

    ALL_OPTIONS.addOption(INPUT_FILE_PATTERN);
    ALL_OPTIONS.addOption(TABLE_NAME);
    ALL_OPTIONS.addOption(KEYSPACE_NAME);
    ALL_OPTIONS.addOption(HOST);
    ALL_OPTIONS.addOption(COLUMNS);
    ALL_OPTIONS.addOption(PARALLELISM);
    ALL_OPTIONS.addOption(BATCH_SIZE);
  }

  public static void main(String[] args) {
    PosixParser parser = new PosixParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      CommandLine commandLine = parser.parse(ALL_OPTIONS, args);
      String filePattern = commandLine.getOptionValue(INPUT_FILE_PATTERN.getOpt());
      String keyspaceName = commandLine.getOptionValue(KEYSPACE_NAME.getOpt());
      String tableName = commandLine.getOptionValue(TABLE_NAME.getOpt());
      String host = commandLine.getOptionValue(HOST.getOpt());
      String columnsInput = commandLine.getOptionValue(COLUMNS.getOpt());

      int parallelism = 1;
      int batchSize = 150;

      if (commandLine.hasOption(PARALLELISM.getOpt())) {
         parallelism = Integer.parseInt(commandLine.getOptionValue(PARALLELISM.getOpt()));
      }

      if (commandLine.hasOption(BATCH_SIZE.getOpt())) {
        batchSize = Integer.parseInt(commandLine.getOptionValue(BATCH_SIZE.getOpt()));
      }

      FileLister lister = new FileLister(filePattern);
      List<File> files = lister.listFiles();

      if (files.isEmpty()) {
        System.out.println("No files matched the pattern " );
        System.exit(-1);
      }

      Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint(host);
      Cluster cluster = clusterBuilder.build();

      ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

      Map<File, Future<Pair<Integer, Long>>> workToExecute = Maps.newHashMap();

      List<String> columns = Arrays.asList(columnsInput.split(","));
      String statement = createStatement(keyspaceName, tableName, columns);

      System.out.println("Prepared (INSERT) statement: " + statement);

      List<DataType> columnTypes = createColumnTypes(cluster, keyspaceName, tableName, columns);

      for (File file: files) {
        workToExecute.put(file, executorService.submit(new LoaderTask(cluster, statement, file,  columnTypes, batchSize)));
      }

      int numFiles = files.size();
      int count = 0;
      for (Map.Entry<File, Future<Pair<Integer, Long>>> workItem : workToExecute.entrySet()) {
         File file = workItem.getKey();
         Future<Pair<Integer,Long>> future = workItem.getValue();
         collectAndReportOnFile(file, future, count, numFiles);
         count++;
      }
    } catch (ParseException e) {
      formatter.printHelp(YbLoader.class.getName(), ALL_OPTIONS, true);
      System.exit(-1);
    }
    System.out.println("Work completed");
    System.exit(0);
  }

  private static String createStatement(String keyspaceName, String tableName, List<String> columns) {
    final Joiner COMMA_JOINER = Joiner.on(",");
    String columnNamesClause = COMMA_JOINER.join(columns.stream().map(String::trim).collect(Collectors.toList()));
    String valuesClause = COMMA_JOINER.join(columns.stream().map(v->"?").collect(Collectors.toList()));
    return String.format("INSERT INTO %s.%s(%s) VALUES(%s)", keyspaceName, tableName, columnNamesClause, valuesClause);
  }

  private static List<DataType> createColumnTypes(Cluster cluster, String keyspaceName, String tableName, List<String> columns) {
    KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(keyspaceName);
    if (keyspace == null) {
      System.err.println("Could not find keyspace " + keyspaceName);
      System.exit(1);
    }

    TableMetadata table = keyspace.getTable(tableName);
    if (table == null) {
      System.err.println("Could not find table " + tableName + " in keyspace " + keyspaceName);
      System.exit(1);
    }
    return columns.stream().map(column->table.getColumn(column).getType()).collect(Collectors.toList());
  }

  private static void collectAndReportOnFile(File file, Future<Pair<Integer, Long>> future, int count, int numFiles) {
    try {
      Pair<Integer, Long> stats = future.get();
      System.out.println("[" + count + "/" + numFiles + "] done. File " + file.getName() + " completed " + stats.getLeft() + " records in " + stats.getRight() + " milliseconds");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        logBatchError(file, cause);
      } else {
        logBatchError(file, e);
      }
    } catch (InterruptedException e) {
      System.out.println("Interrupted while waiting for file " + file.getAbsolutePath() + " to process");
    }
  }

  private static void logBatchError(File file, Throwable e) {
    System.out.println("Batch for file " + file.getName() + "failed. Error: " + e.getMessage());
    e.printStackTrace();
  }

}
