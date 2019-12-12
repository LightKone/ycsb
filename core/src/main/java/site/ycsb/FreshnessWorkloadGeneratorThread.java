/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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
import java.util.HashMap;
import java.util.Random;
import java.util.Properties;

/**
 *
 * A thread for producing an INSERT operation workload.
 * needed for measuring freshness
 *
 */
public class FreshnessWorkloadGeneratorThread extends Thread {
  private DB db;
  private Random r;
  private String table;
  private Boolean doOperations;
  private Map<String, Long> updateTimestamps;

  public FreshnessWorkloadGeneratorThread(DB db, Properties props) {
    this.db = db;
    this.r = new Random();
    this.table = props.getProperty("table", "usertable");
    this.doOperations = true;
  }

  public void run() {
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }
    updateTimestamps = new HashMap<>();
    while (doOperations) {
      double random = r.nextDouble();
      String dbkey = String.format("fr_%s", String.valueOf(random));
      HashMap<String, ByteIterator> values = new HashMap();
      ByteIterator data = new RandomByteIterator(5);
      values.put("content", data);
      Map<String, String> attributes = new HashMap<String, String>();
      attributes.put("f-trip_distance", String.valueOf(random));
      long[] st = new long[1];
      db.insertWithAttributes(table, dbkey, values, attributes, st);
      updateTimestamps.put(dbkey, st[0]);
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return;
      }
    }
  }

  public Map<String, Long> requestStop() {
    doOperations = false;
    return updateTimestamps;
  }
}
