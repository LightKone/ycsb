/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

package site.ycsb.generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import site.ycsb.*;
import site.ycsb.Utils;

/**
 * A generator, whose sequence is the lines of a file.
 */
public class AttributeGenerator extends Generator<List<Map<String, String>>> {
  public static final String QUERY_START_VALUE_DISTRIBUTION_PROPERTY = "querystartvaluedistribution";
  public static final String QUERY_START_VALUE_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
  public static final String MIN_QUERY_START_VALUE_PROPERTY = "minquerystartvalue";
  public static final String MIN_QUERY_START_VALUE_PROPERTY_DEFAULT = "0.0";
  public static final String MAX_QUERY_START_VALUE_PROPERTY = "maxquerystartvalue";
  public static final String MAX_QUERY_START_VALUE_PROPERTY_DEFAULT = "20.0";
  public static final String QUERY_RANGE_DISTRIBUTION_PROPERTY = "queryrangedistribution";
  public static final String QUERY_RANGE_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
  public static final String MIN_QUERY_RANGE_PROPERTY = "minquerystartvalue";
  public static final String MIN_QUERY_RANGE_PROPERTY_DEFAULT = "0.0";
  public static final String MAX_QUERY_RANGE_PROPERTY = "maxquerystartvalue";
  public static final String MAX_QUERY_RANGE_PROPERTY_DEFAULT = "20.0";
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";
  public static final String CACHED_QUERY_PROPORTION_PROPERTY = "cachedqueryproportion";
  public static final String CACHED_QUERY_PROPORTION_PROPERTY_DEFAULT = "0.2";
  public static final String CACHE_SIZE_PROPERTY = "cachesize";
  public static final String CACHE_SIZE_PROPERTY_DEFAULT = "10";
  public static final String INSERT_START_PROPERTY = "insertstart";
  public static final String INSERT_START_PROPERTY_DEFAULT = "0";
  public static final String INSERT_ORDER_PROPERTY = "insertorder";
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";
  public static final String ZERO_PADDING_PROPERTY = "zeropadding";
  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";
  public static final String QUERY_TYPE_PROPERTY = "querytype";
  public static final String QUERY_TYPE_PROPERTY_DEFAULT = "point";

  private static AttributeGenerator instance = null;
  private final String filename;
  private String line;
  private int current;
  private int recordCount;
  protected long attributecount;
  private List<Map<String, String>> currentDatasetEntry;
  private BufferedReader reader;
  private HashMap<Double, Integer> tripDistanceValues;
  private ArrayList<Double> tripDistanceValuesArr;
  protected NumberGenerator lBoundChooser;
  protected NumberGenerator rangeChooser;
  protected DiscreteGenerator latestQueryChooser;
  private PreviousQueries prevQueries;
  protected NumberGenerator keysequence;
  protected int zeropadding;
  protected boolean orderedinserts;
  protected boolean queryTypeRange;
  private NumberGenerator pointQueryValueGenerator;

  /**
   * Create a AttributeGenerator with the given file.
   * @param filename The file to read lines from.
   */
  public AttributeGenerator(String filename, int recordCount, Properties p) {
    this.filename = filename;
    this.recordCount = recordCount;
    this.tripDistanceValues = new HashMap<Double, Integer>();
    this.tripDistanceValuesArr = new ArrayList();
    pointQueryValueGenerator = new UniformLongGenerator(0, recordCount-1);
    int cachesize =
        Integer.parseInt(p.getProperty(CACHE_SIZE_PROPERTY, CACHE_SIZE_PROPERTY_DEFAULT));
    this.prevQueries = new PreviousQueries(cachesize);
    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    keysequence = new CounterGenerator(insertstart);
    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else {
      orderedinserts = true;
    }
    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));
    if (p.getProperty(QUERY_TYPE_PROPERTY, QUERY_TYPE_PROPERTY_DEFAULT).compareTo("range") == 0) {
      queryTypeRange = true;
    } else {
      queryTypeRange = false;
    }
    String querystartvaluedistrib =
        p.getProperty(QUERY_START_VALUE_DISTRIBUTION_PROPERTY, QUERY_START_VALUE_DISTRIBUTION_PROPERTY_DEFAULT);
    double minquerystartvalue =
        Double.parseDouble(p.getProperty(MIN_QUERY_START_VALUE_PROPERTY, MIN_QUERY_START_VALUE_PROPERTY_DEFAULT));
    double maxquerystartvalue =
        Double.parseDouble(p.getProperty(MAX_QUERY_START_VALUE_PROPERTY,
                                                  MAX_QUERY_START_VALUE_PROPERTY_DEFAULT));
    String queryrangeistrib =
        p.getProperty(QUERY_RANGE_DISTRIBUTION_PROPERTY, QUERY_RANGE_DISTRIBUTION_PROPERTY_DEFAULT);
    double minqueryrange =
        Double.parseDouble(p.getProperty(MIN_QUERY_RANGE_PROPERTY, MIN_QUERY_RANGE_PROPERTY_DEFAULT));
    double maxqueryrange =
        Double.parseDouble(p.getProperty(MAX_QUERY_RANGE_PROPERTY, MAX_QUERY_RANGE_PROPERTY_DEFAULT));

    if (querystartvaluedistrib.compareTo("uniform") == 0) {
      lBoundChooser = new UniformLongGenerator((long) minquerystartvalue*5, (long) maxquerystartvalue*5);
    } else if (querystartvaluedistrib.compareTo("zipfian") == 0) {
      lBoundChooser = new ZipfianGenerator((long) minquerystartvalue*5, (long) maxquerystartvalue*5);
    } else if (querystartvaluedistrib.compareTo("latest") == 0) {
      throw new UnsupportedOperationException("Not yet implemented \"" + querystartvaluedistrib + "\"");
    } else if (querystartvaluedistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      lBoundChooser = new HotspotIntegerGenerator((long) minquerystartvalue*5, (long) maxquerystartvalue*5,
          hotsetfraction, hotopnfraction);
    } else {
      throw new UnsupportedOperationException("Unknown request distribution \"" + querystartvaluedistrib + "\"");
    }
    if (queryrangeistrib.compareTo("uniform") == 0) {
      rangeChooser = new UniformLongGenerator((long) minqueryrange*5, (long) maxqueryrange*5);
    } else if (queryrangeistrib.compareTo("zipfian") == 0) {
      rangeChooser = new ZipfianGenerator((long) minquerystartvalue*5, (long) maxquerystartvalue*5);
    } else if (queryrangeistrib.compareTo("latest") == 0) {
      throw new UnsupportedOperationException("Not yet implemented \"" + querystartvaluedistrib + "\"");
    } else if (queryrangeistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      rangeChooser = new HotspotIntegerGenerator((long) minquerystartvalue*5, (long) maxquerystartvalue*5,
          hotsetfraction, hotopnfraction);
    } else {
      throw new UnsupportedOperationException("Unknown request distribution \"" + queryrangeistrib + "\"");
    }
    latestQueryChooser = new DiscreteGenerator();
    double cachedqueryfraction =
          Double.parseDouble(p.getProperty(CACHED_QUERY_PROPORTION_PROPERTY, CACHED_QUERY_PROPORTION_PROPERTY_DEFAULT));
    latestQueryChooser.addValue(cachedqueryfraction, "cached");
    latestQueryChooser.addValue(1.0 - cachedqueryfraction, "new");

    reloadFile();
    try {
      reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (int i=0; i<insertstart; i++) {
      try {
        line = reader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    boolean dotransactions = Boolean.valueOf(p.getProperty(Client.DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));
    if (dotransactions) {
      preload();
    }
  }

  public static AttributeGenerator getInstance(String filename, int recordCount, Properties p) {
    if (instance == null) {
      instance = new AttributeGenerator(filename, recordCount, p);
    }
    return instance;
  }

  private void preload() {
    for(int i=0; i<recordCount; i++) {
      int keynum = keysequence.nextValue().intValue();
      String dbkey = buildKeyName(keynum);
      List<Map<String, String>> attributeList = nextValue();
      tripDistanceInsert(Double.parseDouble(attributeList.get(4).get("f-trip_distance")));
    }
  }

  public void nextQuery(String []attributeName, String []attributeType,  java.lang.Object []lbound,
                              java.lang.Object []ubound) {
    String query = latestQueryChooser.nextString();
    if(query == null) {
      throw new AssertionError("nextQuery null");
    }
    if (prevQueries.isEmpty()) {
      query = "new";
    }
    switch (query) {
    case "new":
      newQuery(attributeName, attributeType, lbound, ubound);
      break;
    case "cached":
      prevQueries.nextQuery(attributeName, attributeType, lbound, ubound);
      break;
    default:
      throw new AssertionError("nextQuery neither 'new' nor 'cached'");
    }
  }

  private void newQuery(String []attributeName, String []attributeType,  java.lang.Object []lbound,
                    java.lang.Object []ubound) {
    attributeName[0] = "trip_distance";
    attributeType[0] = "S3TAGFLT";
    if (queryTypeRange) {
      throw new AssertionError("range Queries not implemented'");
    } else {
      int queryValueIndex = pointQueryValueGenerator.nextValue().intValue();
      double value = tripDistanceGet(queryValueIndex);
      lbound[0] = String.valueOf(value);
      ubound[0] = String.valueOf(value);
    }
    prevQueries.addQuery(attributeName, attributeType, new String[]{(java.lang.String) lbound[0]},
                        new String[]{(java.lang.String) lbound[0]});
  }
  /**
   * Return the next string of the sequence, ie the next line of the file.
   */
  @Override
  public synchronized List<Map<String, String>> nextValue() {
    currentDatasetEntry = next();
    return currentDatasetEntry;
  }


  private synchronized List<Map<String, String>> next() {
    try {
      line = reader.readLine();
      String [] data = line.split(",");
      DatasetEntry userMetadata = new DatasetEntry();
      double tripDistanceVal = userMetadata.set(data);
      return userMetadata.get();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the previous read line.
   */
  @Override
  public List<Map<String , String>> lastValue() {
    return currentDatasetEntry;
  }

  private String buildKeyName(long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "user";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }


  /**
   * Reopen the file to reuse values.
   */
  public synchronized void reloadFile() {
    try (Reader r = reader) {
      reader = new BufferedReader(new FileReader(filename));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void tripDistanceInsert(double value) {
    if (!tripDistanceValues.containsKey(value)) {
      tripDistanceValues.put(value, new Integer(0));
      tripDistanceValuesArr.add(value);
    }
  }

  private synchronized double tripDistanceGet(int index) {
    return tripDistanceValuesArr.get(index % tripDistanceValues.size());
  }
}

class PreviousQueries {
  private static class Query {
    private String[] attributeName;
    private String[] attributeType;
    private String[] lbound;
    private String[] ubound;
    private double weight;

    Query(String []attributeName, String []attributeType, String []lbound, String []ubound) {
      this.attributeName = attributeName;
      this.attributeType = attributeType;
      this.lbound = lbound;
      this.ubound = ubound;
    }
  }

  private final List<Query> queries = new ArrayList<>();
  private final int cacheSize;

  PreviousQueries(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  public boolean isEmpty() {
    return queries.size() == 0;
  }
  public synchronized void addQuery(String []attributeName, String []attributeType, String []lbound, String []ubound) {
    if (queries.size() >= cacheSize) {
      do {
        queries.remove(0);
      }
      while (queries.size() >= cacheSize);
    }
    queries.add(0, new Query(attributeName, attributeType, lbound, ubound));
    double w = 1.0;
    for(int i=0; i<queries.size(); i++) {
      w = 1.0/(i+1);
      Query q = queries.get(i);
      q.weight = w;
      queries.set(i, q);
    }
  }

  public synchronized void nextQuery(String []attributeName, String []attributeType,  java.lang.Object []lbound,
                              java.lang.Object []ubound) {
    double sum = 0;
    for (Query q : queries) {
      sum += q.weight;
    }
    double val = ThreadLocalRandom.current().nextDouble();
    for (Query q : queries) {
      double pw = q.weight / sum;
      if (val < pw) {
        for (int i=0; i<attributeName.length; i++) {
          attributeName[i] = new String(q.attributeName[i]);
          attributeType[i] = new String(q.attributeType[i]);
          lbound[i] = new String(q.lbound[i]);
          ubound[i] = new String(q.ubound[i]);
        }
        queries.remove(q);
        addQuery(q.attributeName, q.attributeType, q.lbound, q.ubound);
        return;
      }
      val -= pw;
    }
    throw new AssertionError("oops. should not get here.");
  }
}

class DatasetEntry {
  private List<Map<String, String>> entry;

  DatasetEntry(){
    entry = new ArrayList<Map<String, String>>();
  }

  DatasetEntry(List<Map<String, String>> e) {
    entry = e;
  }

  public double set(String[] data) {
    Map<String, String> attribute0 = new HashMap<String, String>();
    attribute0.put("vendorid", data[0]);
    entry.add(0, attribute0);
    Map<String, String> attribute1 = new HashMap<String, String>();
    attribute1.put("tpep_pickup_datetime", data[1]);
    entry.add(1, attribute1);
    Map<String, String> attribute2 = new HashMap<String, String>();
    attribute2.put("tpep_dropoff_datetime", data[2]);
    entry.add(2, attribute2);
    Map<String, String> attribute3 = new HashMap<String, String>();
    attribute3.put("i-passenger_count", data[3]);
    entry.add(3, attribute3);
    Map<String, String> attribute4 = new HashMap<String, String>();
    attribute4.put("f-trip_distance", data[4]);
    entry.add(4, attribute4);
    Map<String, String> attribute5 = new HashMap<String, String>();
    attribute5.put("ratecodeid", data[5]);
    entry.add(5, attribute5);
    Map<String, String> attribute6 = new HashMap<String, String>();
    attribute6.put("store_and_fwd_flag", data[6]);
    entry.add(6, attribute6);
    Map<String, String> attribute7 = new HashMap<String, String>();
    attribute7.put("pulocationid", data[7]);
    entry.add(7, attribute7);
    Map<String, String> attribute8 = new HashMap<String, String>();
    attribute8.put("doLocationid", data[8]);
    entry.add(8, attribute8);
    Map<String, String> attribute9 = new HashMap<String, String>();
    attribute9.put("payment_type", data[9]);
    entry.add(9, attribute9);
    Map<String, String> attribute10 = new HashMap<String, String>();
    attribute10.put("fare_amount", data[10]);
    entry.add(10, attribute10);
    Map<String, String> attribute11 = new HashMap<String, String>();
    attribute11.put("extra", data[11]);
    entry.add(11, attribute11);
    Map<String, String> attribute12 = new HashMap<String, String>();
    attribute12.put("mta_tax", data[12]);
    entry.add(12, attribute12);
    Map<String, String> attribute13 = new HashMap<String, String>();
    attribute13.put("tip_amount", data[13]);
    entry.add(13, attribute13);
    Map<String, String> attribute14 = new HashMap<String, String>();
    attribute14.put("tolls_amount", data[14]);
    entry.add(14, attribute14);
    Map<String, String> attribute15 = new HashMap<String, String>();
    attribute15.put("improvement_surcharge", data[15]);
    entry.add(15, attribute15);
    Map<String, String> attribute16 = new HashMap<String, String>();
    attribute16.put("total_amount", data[16]);
    entry.add(16, attribute16);
    Map<String, String> attribute17 = new HashMap<String, String>();
    attribute17.put("congestion_surcharge", data[17]);
    entry.add(17, attribute17);
    return (double) Double.parseDouble(data[4]);
  }

  public double getTripDistance() {
    Map<String, String> tripDistanceAttribute = entry.get(4);
    return (double) Double.parseDouble(tripDistanceAttribute.get("f-trip_distance"));
  }

  public List<Map<String, String>> get() {
    return entry;
  }
}