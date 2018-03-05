package com.yugabyte.ybloader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class YbLoader {


  public static void main(String[] args) {
    if (args.length != 5) {
      System.out.println("Usage ./foo host infile keyspace table \"col1, col2, .., coln\"");
      System.exit(1);
    }

    Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint(args[0]);
    Cluster cluster = clusterBuilder.build();
    File inFile = new File(args[1]);
    load(cluster, inFile, args[2], args[3], args[4]);
  }

  /**
   * Load data from an input file to a given table.
   * @param cluster The Cluster object
   * @param inFile The input file
   * @param keyspaceName The name of the target keyspace
   * @param tableName The name of the target table
   * @param inCols The columns whose values are given in the input file (in that order)
   */
  private static void load(Cluster cluster, File inFile, String keyspaceName, String tableName, String inCols) {
    Session session = cluster.newSession();

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


    List<DataType> colTypes = new ArrayList<DataType>();
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    for (String col : inCols.split(",")) {
      col = col.trim();

      DataType colType = table.getColumn(col).getType();
      colTypes.add(colType);
      if (!first) {
        sb.append(',');
      } else {
        first = false;
      }
      sb.append('?');
    }
    String valuesClause = sb.toString();

    String stmtS = String.format("INSERT INTO %s.%s(%s) VALUES(%s)", keyspaceName, tableName, inCols, valuesClause);
    System.out.println(stmtS);
    PreparedStatement statement = session.prepare(stmtS);

    LoaderTask loader = new LoaderTask(session, statement, inFile, colTypes);
    try {
      loader.load();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    session.close();
    cluster.close();
  }
}
