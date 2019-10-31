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

import java.util.List;

/**
 * A thread that waits for the maximum specified time and then interrupts all the client
 * threads passed at initialization of this thread.
 *
 * The maximum execution time passed is assumed to be in seconds.
 *
 */
public class WarmupPeriodThread extends Thread {

  private List<ClientThread> clients;
  private long warmupTime;
  private Workload workload;
  private long waitTimeOutInMS;

  public WarmupPeriodThread(long warmupTime, List<ClientThread> clients) {
    this.warmupTime = warmupTime;
    this.clients = clients;
    System.err.println("Warmup time specified as: " + warmupTime + " secs");
  }

  public void run() {
    try {
      Thread.sleep(warmupTime * 1000);
    } catch (InterruptedException e) {
      System.err.println("Could not wait until specified warm up time, WarmupPeriodThread interrupted.");
      return;
    }
    for (ClientThread client : clients) {
      client.endWarmup();
    }
    System.err.println("Warm up time elapsed. Starting measuring.");
  }
}
