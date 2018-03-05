package com.yugabyte.ybloader;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;


/**
 * Load one file.
 */
class LoaderTask {

  private Session session;
  private PreparedStatement statement;
  private BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

  private List<DataType> colTypes;
  private File inFile;

  // Settings
  private int batchSize = 150;
  private int logStatsFrequency = 2000; // log every n rows processed.
  private boolean enableExtraLogging = false;

  // Stats
  private long startTimeMillis;
  private int numRowsProcessed = 0;

  // Parsing Settings
  private char valueSep = ',';
  private char pairSep = ':'; // Used inside map and udt literals
  private char collStart = '{'; // Start marker for all collections except list.
  private char collEnd = '}'; // End marker for all collections except list.

  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSX", Locale.ENGLISH);

  // Parsing State
  private String currentLine;
  private int currentPos = 0; // within the current line

  LoaderTask(Session session, PreparedStatement statement, File inFile, List<DataType> colTypes) {
    this.session = session;
    this.statement = statement;
    this.inFile = inFile;
    this.colTypes = colTypes;
  }

  /**
   * Traverse input file and process each line.
   */
  void load() throws IOException {
    FileInputStream inputStream = null;
    Scanner sc = null;
    try {
      inputStream = new FileInputStream(inFile);
      sc = new Scanner(inputStream, "UTF-8");
      startTimeMillis = System.currentTimeMillis();
      while (sc.hasNextLine()) {
        String line = sc.nextLine();
        processLine(line);
        numRowsProcessed++;
        if (numRowsProcessed % logStatsFrequency == 0) {
          long duration = System.currentTimeMillis() - startTimeMillis;
          System.out.println("Processed " + numRowsProcessed + " rows in " +
              duration / 1000 + "." + (duration % 1000 / 10) + " seconds");
        }
      }
      // If there are leftover stmts in the batch, execute them.
      if (batch.size() > 0) {
        session.executeAsync(batch);
      }
      // If there are leftover, unlogged rows, report them.
      if (numRowsProcessed % logStatsFrequency != 0) {
        long duration = System.currentTimeMillis() - startTimeMillis;
        System.out.println("Processed " + numRowsProcessed + " rows in " +
            duration / 1000 + "." + (duration % 1000 / 10) + " seconds");
      }
      // note that Scanner suppresses exceptions
      if (sc.ioException() != null) {
        throw sc.ioException();
      }
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
      if (sc != null) {
        sc.close();
      }
    }
  }

  /**
   * Parse one line and add the resulting insert stmt to the batch.
   * Execute the batch when the batchSize is reached.
   * Leftover stmts (if any) will be executed at the end in the load() function.
   * @param line the current line.
   */
  private void processLine(String line) {
    currentPos = 0;
    currentLine = line;
    try {
      Object[] values = new Object[colTypes.size()];
      int i = 0;
      for (DataType type : colTypes) {
        values[i] = readValue(type);
        currentPos++; // Skip separator to get to next value;
        log("Got value: " + values[i].toString());
        i++;
      }
      batch.add(statement.bind(values));
      if (batch.size() >= batchSize) {
        session.executeAsync(batch);
        batch.clear();
      }
    } catch (Exception e) {
      System.err.println("Skipping line: \"" + currentLine + "\" due to error: " + e.getMessage());
    }
  }

  /**
   * Parse one value and return the corresponding CQL object.
   * Can be a simple value or a Collection/UDT.
   * @param type the expected CQL Type for the value.
   * @return the parsed value.
   * @throws java.text.ParseException
   */
  private Object readValue(DataType type) throws java.text.ParseException {
    switch (type.getName()) {
      case TINYINT:
        return Byte.valueOf(parseSimpleValue(valueSep, '\n'));
      case SMALLINT:
        return Short.valueOf(parseSimpleValue(valueSep, '\n'));
      case INT:
        return Integer.valueOf(parseSimpleValue(valueSep, '\n'));
      case BIGINT:
        return Long.valueOf(parseSimpleValue(valueSep, '\n'));
      case FLOAT:
        return Float.valueOf(parseSimpleValue(valueSep, '\n'));
      case DOUBLE:
        return Double.valueOf(parseSimpleValue(valueSep, '\n'));
      case BOOLEAN:
        return Boolean.valueOf(parseSimpleValue(valueSep, '\n'));
      case TEXT:
        return parseSimpleValue(valueSep, '\n');
      case TIMESTAMP:
        return dateFormat.parse(parseSimpleValue(valueSep, '\n'));
      case UUID:
        return UUID.fromString(parseSimpleValue(valueSep, '\n'));
      case UDT:
        UserType udt = (UserType) type;
        return parseUDTValue(udt);

      default:
        System.err.println("Column Type " + type.getName().toString() + " not yet supported.");
        System.exit(1);
        return null;
    }
  }

  /**
   * Parse a simple value, traverse string until the next separator or the end marker (if this is
   * the last value).
   * @param valueSep the separator, generally ',' but can be e,g, ':' inside maps/udts
   * @param endMarker mostly used for nested values like collections/udts (e.g. '}', or ']').
   * @return the resulting String.
   */
  private String parseSimpleValue(char valueSep, char endMarker) {
    StringBuilder sb = new StringBuilder();
    boolean inQuote = false;
    while (currentPos < currentLine.length() && (inQuote ||
        (currentLine.charAt(currentPos) != valueSep && currentLine.charAt(currentPos) != endMarker))) {
      if (currentLine.charAt(currentPos) == '"') {
        if (inQuote && currentPos + 1 < currentLine.length() && currentLine.charAt(currentPos +1) == '"') {
          currentPos++;
        } else {
          inQuote = !inQuote;
        }
      }
      sb.append(currentLine.charAt(currentPos));
      currentPos++;
    }

    log("Parsed simple value: " + sb.toString());
    return sb.toString();
  }

  /**
   * Parse a literal for a User-Defined Type.
   * @param type the User-Defined Type
   * @return the UDTValue object.
   * @throws java.text.ParseException
   */
  private UDTValue parseUDTValue(UserType type) throws java.text.ParseException {
    UDTValue udtValue = type.newValue();
    int start = currentPos;

    boolean isQuoted = false;

    // Find start
    while (currentLine.charAt(currentPos) != collStart) {
      if (currentLine.charAt(currentPos) == '"' && !isQuoted) {
        isQuoted = true;
      } else if (currentLine.charAt(currentPos) != ' ') {
        System.err.println("Invalid UDT value: " + currentLine.substring(start, currentPos +1));
        System.exit(1);
      }
      currentPos++;
    }
    currentPos++;

    boolean done = false;

    while (!done) {
      String fieldName = parseSimpleValue(pairSep, '\n').trim();
      DataType fieldType = type.getFieldType(fieldName);
      currentPos++; // Skip separator
      String valueString = parseSimpleValue(valueSep, collEnd).trim();
      if (currentLine.charAt(currentPos) == collEnd) {
        done = true;
      }

      if (!valueString.isEmpty()) {
        switch (fieldType.getName()) {
          case TINYINT:
            udtValue.setByte(fieldName, Byte.valueOf(valueString));
            break;
          case SMALLINT:
            udtValue.setShort(fieldName, Short.valueOf(valueString));
            break;
          case INT:
            udtValue.setInt(fieldName, Integer.valueOf(valueString));
            break;
          case BIGINT:
            udtValue.setLong(fieldName, Long.valueOf(valueString));
            break;
          case FLOAT:
            udtValue.setFloat(fieldName, Float.valueOf(valueString));
            break;
          case DOUBLE:
            udtValue.setDouble(fieldName, Double.valueOf(valueString));
            break;
          case BOOLEAN:
            udtValue.setBool(fieldName, Boolean.valueOf(valueString));
            break;
          case TEXT:
            udtValue.setString(fieldName, valueString);
            break;
          case TIMESTAMP:
            udtValue.setTimestamp(fieldName, dateFormat.parse(valueString));
            break;

          default:
            System.err.println("Type " + fieldType.getName().toString() + " not yet supported in UDT.");
            System.exit(1);
        }
      }
      currentPos++; // Skip separator
    }

    if (isQuoted) {
      skipWhitespace();
      if (currentLine.charAt(currentPos) != '"') {
        throw new java.text.ParseException("Expected ending quote for UDT", currentPos);
      }
      currentPos++;
    }

    return udtValue;
  }

  // Utilities

  private void log(String msg) {
    if (enableExtraLogging) {
      System.out.println(msg);
    }
  }

  private void skipWhitespace() {
    while (currentLine.charAt(currentPos) == ' ') {
      currentPos++;
    }
  }

}
