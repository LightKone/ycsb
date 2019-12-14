/**
 * Copyright (c) 2010 Yahoo! Inc., 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import java.util.Map;
import site.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.util.*;

import java.util.concurrent.CountDownLatch;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB {
  private final DB db;
  private final Measurements measurements;
  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;
  private Set<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private static final String WARMPUP_TIME = "warmuptime";

  private final String scopeStringCleanup;
  private final String scopeStringDelete;
  private final String scopeStringInit;
  private final String scopeStringInsert;
  private final String scopeStringRead;
  private final String scopeStringScan;
  private final String scopeStringUpdate;
  private final String scopeStringReadWithAttributes;
  private final String scopeStringInsertWithAttributes;
  private final String scopeStringUpdateWithAttributes;
  private final String scopeStringQuery;

  private boolean warmup = false;

  public DBWrapper(final DB db, final Tracer tracer) {
    this.db = db;
    measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    scopeStringCleanup = simple + "#cleanup";
    scopeStringDelete = simple + "#delete";
    scopeStringInit = simple + "#init";
    scopeStringInsert = simple + "#insert";
    scopeStringRead = simple + "#read";
    scopeStringScan = simple + "#scan";
    scopeStringUpdate = simple + "#update";
    scopeStringInsertWithAttributes = simple + "#insertWithAttributes";
    scopeStringReadWithAttributes = simple + "#readWithAttributes";
    scopeStringUpdateWithAttributes = simple + "#updateWithAttributes";
    scopeStringQuery = simple + "#query";
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return db.getProperties();
  }

  public synchronized void endWarmup() {
    warmup = false;
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringInit)) {
      db.init();

      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
          getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
              REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
              latencyTrackedErrorsProperty.split(",")));
        }
      }
      long warmupTime = Integer.parseInt(getProperties().getProperty(WARMPUP_TIME, "0"));
      if (warmupTime > 0) {
        warmup = true;
      }

      System.err.println("DBWrapper: report latency for each error is " +
          this.reportLatencyForEachError + " and specific error codes to track" +
          " for latency are: " + this.latencyTrackedErrors.toString());
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try (final TraceScope span = tracer.newScope(scopeStringRead)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.read(table, key, fields, result);
      long en = System.nanoTime();
      if (!warmup) {
        measure("READ", res, ist, st, en);
        measurements.reportStatus("READ", res);
      }
      return res;
    }
  }

  public Status readWithAttributes(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result,
                     Map<String, String> attributes) {
    try (final TraceScope span = tracer.newScope(scopeStringReadWithAttributes)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.readWithAttributes(table, key, fields, result, attributes);
      long en = System.nanoTime();
      if (!warmup) {
        measure("READ_WITH_ATTRIBUTES", res, ist, st, en);
        measurements.reportStatus("READ_WITH_ATTRIBUTES", res);
      }
      return res;
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try (final TraceScope span = tracer.newScope(scopeStringScan)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.scan(table, startkey, recordcount, fields, result);
      long en = System.nanoTime();
      if (!warmup) {
        measure("SCAN", res, ist, st, en);
        measurements.reportStatus("SCAN", res);
      }
      return res;
    }
  }

  private void measure(String op, Status result, long intendedStartTimeNanos,
                       long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    measurements.measure(measurementName,
        (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(measurementName,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringUpdate)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.update(table, key, values);
      long en = System.nanoTime();
      if (!warmup) {
        measure("UPDATE", res, ist, st, en);
        measurements.reportStatus("UPDATE", res);
      }
      return res;
    }
  }

  public Status updateWithAttributes(String table, String key,
                                    Map<String, ByteIterator> values,
                                    Map<String, String> attributes) {
    try (final TraceScope span = tracer.newScope(scopeStringUpdateWithAttributes)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.updateWithAttributes(table, key, values, attributes);
      long en = System.nanoTime();
      if (!warmup) {
        measure("UPDATE_WITH_ATTRIBUTES", res, ist, st, en);
        measurements.reportStatus("UPDATE_WITH_ATTRIBUTES", res);
      }
      return res;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try (final TraceScope span = tracer.newScope(scopeStringInsert)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.insert(table, key, values);
      long en = System.nanoTime();
      if (!warmup) {
        measure("INSERT", res, ist, st, en);
        measurements.reportStatus("INSERT", res);
      }
      return res;
    }
  }

  public Status insertWithAttributes(String table, String key,
                                    Map<String, ByteIterator> values,
                                    Map<String, String> attributes,
                                    long []stTs) {
    try (final TraceScope span = tracer.newScope(scopeStringInsertWithAttributes)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.insertWithAttributes(table, key, values, attributes, null);
      long en = System.nanoTime();
      if (stTs != null) {
        stTs[0] = en;
      }
      if (!warmup) {
        measure("INSERT_WITH_ATTRIBUTES", res, ist, st, en);
        measurements.reportStatus("INSERT_WITH_ATTRIBUTES", res);
      }
      return res;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public Status delete(String table, String key) {
    try (final TraceScope span = tracer.newScope(scopeStringDelete)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.delete(table, key);
      long en = System.nanoTime();
      if (!warmup) {
        measure("DELETE", res, ist, st, en);
        measurements.reportStatus("DELETE", res);
      }
      return res;
    }
  }

  public Status query(String []attributeName, String []attributeType,  java.lang.Object []lbound,
                              java.lang.Object []ubound, long []en) {
    try (final TraceScope span = tracer.newScope(scopeStringQuery)) {
      long ist = measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = db.query(attributeName, attributeType, lbound, ubound, en);
      if (!warmup) {
        if (res == Status.OK) {
          measure("QUERY", res, ist, st, en[0]);
        }
        measurements.reportStatus("QUERY", res);
      }
      return res;
    }
  }

  public Status subscribeQuery(String []attributeName, String []attributeType,  java.lang.Object []lbound,
                              java.lang.Object []ubound, CountDownLatch finishLatch) {
    Status res = db.subscribeQuery(attributeName, attributeType, lbound, ubound, finishLatch);
    return res;
  }
}
