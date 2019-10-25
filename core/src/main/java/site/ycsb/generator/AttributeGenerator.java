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

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
/**
 * A generator, whose sequence is the lines of a file.
 */
public class AttributeGenerator extends Generator<List<Map<String, String>>> {
  private final String filename;
  private String line;
  private List<Map<String, String>> current;
  private BufferedReader reader;

  /**
   * Create a AttributeGenerator with the given file.
   * @param filename The file to read lines from.
   */
  public AttributeGenerator(String filename) {
    this.filename = filename;
    reloadFile();
  }

  /**
   * Return the next string of the sequence, ie the next line of the file.
   */
  @Override
  public synchronized List<Map<String , String>> nextValue() {
    try {
      line = reader.readLine();
      String [] data = line.split(",");

      List<Map<String , String>> userMetadata = new ArrayList<Map<String, String>>();

      Map<String, String> attribute0 = new HashMap<String, String>();
      attribute0.put("vendorid", data[0]);
      userMetadata.add(0, attribute0);
      Map<String, String> attribute1 = new HashMap<String, String>();
      attribute1.put("tpep_pickup_datetime", data[1]);
      userMetadata.add(1, attribute1);
      Map<String, String> attribute2 = new HashMap<String, String>();
      attribute2.put("tpep_dropoff_datetime", data[2]);
      userMetadata.add(2, attribute2);
      Map<String, String> attribute3 = new HashMap<String, String>();
      attribute3.put("passenger_count", data[3]);
      userMetadata.add(3, attribute3);
      Map<String, String> attribute4 = new HashMap<String, String>();
      attribute4.put("trip_distance", data[4]);
      userMetadata.add(4, attribute4);
      Map<String, String> attribute5 = new HashMap<String, String>();
      attribute5.put("ratecodeid", data[5]);
      userMetadata.add(5, attribute5);
      Map<String, String> attribute6 = new HashMap<String, String>();
      attribute6.put("store_and_fwd_flag", data[6]);
      userMetadata.add(6, attribute6);
      Map<String, String> attribute7 = new HashMap<String, String>();
      attribute7.put("pulocationid", data[7]);
      userMetadata.add(7, attribute7);
      Map<String, String> attribute8 = new HashMap<String, String>();
      attribute8.put("doLocationid", data[8]);
      userMetadata.add(8, attribute8);
      Map<String, String> attribute9 = new HashMap<String, String>();
      attribute9.put("payment_type", data[9]);
      userMetadata.add(9, attribute9);
      Map<String, String> attribute10 = new HashMap<String, String>();
      attribute10.put("fare_amount", data[10]);
      userMetadata.add(10, attribute10);
      Map<String, String> attribute11 = new HashMap<String, String>();
      attribute11.put("extra", data[11]);
      userMetadata.add(11, attribute11);
      Map<String, String> attribute12 = new HashMap<String, String>();
      attribute12.put("mta_tax", data[12]);
      userMetadata.add(12, attribute12);
      Map<String, String> attribute13 = new HashMap<String, String>();
      attribute13.put("tip_amount", data[13]);
      userMetadata.add(13, attribute13);
      Map<String, String> attribute14 = new HashMap<String, String>();
      attribute14.put("tolls_amount", data[14]);
      userMetadata.add(14, attribute14);
      Map<String, String> attribute15 = new HashMap<String, String>();
      attribute15.put("improvement_surcharge", data[15]);
      userMetadata.add(15, attribute15);
      Map<String, String> attribute16 = new HashMap<String, String>();
      attribute16.put("total_amount", data[16]);
      userMetadata.add(16, attribute16);
      Map<String, String> attribute17 = new HashMap<String, String>();
      attribute17.put("congestion_surcharge", data[17]);
      userMetadata.add(17, attribute17);

      current = userMetadata;
      return current;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the previous read line.
   */
  @Override
  public List<Map<String , String>> lastValue() {
    return current;
  }

  /**
   * Reopen the file to reuse values.
   */
  public synchronized void reloadFile() {
    try (Reader r = reader) {
      System.err.println("Reload " + filename);
      reader = new BufferedReader(new FileReader(filename));
//  String csvFile = "/Users/dimvas/Downloads/yellow_tripdata_2019-06.csv";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
