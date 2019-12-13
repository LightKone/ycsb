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
import java.util.concurrent.CountDownLatch;
import java.util.Properties;

import site.ycsb.measurements.Measurements;

/**
 *
 * A thread for measuring the freshness of results provided by the query processing system.
 *
 */
public class FreshnessMeasurementThread extends Thread {
  private DB db;
  private FreshnessWorkloadGeneratorThread workloadGenerator;
  private CountDownLatch finishLatch;
  private Map<String, Long> notificationTimestamps;
  private Measurements measurements = Measurements.getMeasurements();

  public FreshnessMeasurementThread(DB db, Properties props) {
    this.db = db;
    try {
      db.init();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }
    this.workloadGenerator = new FreshnessWorkloadGeneratorThread(db, props);
    this.notificationTimestamps = new HashMap<>();
    this.finishLatch = new CountDownLatch(2);
  }

  public void run() {
    workloadGenerator.start();
    String[] attributeName = new String[1];
    String[] attributeType = new String[1];
    attributeName[0] = "trip_distance";
    attributeType[0] = "S3TAGFLT";
    java.lang.Object[] lbound = new java.lang.Object[1];
    java.lang.Object[] ubound = new java.lang.Object[1];
    double lb = 0.0;
    double ub = 1.0;
    lbound[0] = String.valueOf(lb);
    ubound[0] = String.valueOf(ub);
    db.subscribeQuery(attributeName, attributeType, lbound, ubound, notificationTimestamps, finishLatch);
  }

  public void requestStop() {
    Map<String, Long> updateTimestamps = workloadGenerator.requestStop();
    finishLatch.countDown();
    notificationTimestamps.entrySet().forEach(entry->{
        if (updateTimestamps.containsKey(entry.getKey())) {
          long updateTs = updateTimestamps.get(entry.getKey());
          long notificationTs = notificationTimestamps.get(entry.getKey());
          int freshness = (int) ((notificationTs - updateTs) / 1000);
          measurements.measure("FRESHNESS_LATENCY", (int) ((notificationTs - updateTs) / 1000));
          measurements.reportStatus("FRESHNESS_LATENCY", Status.OK);
        }
      });
  }
}
