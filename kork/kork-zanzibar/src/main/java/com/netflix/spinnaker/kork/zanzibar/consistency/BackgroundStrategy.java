/*
 * Copyright 2026 Apple, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.kork.zanzibar.consistency;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** A strategy's background thread, and warnings at most once a minute. */
abstract class BackgroundStrategy implements ConsistencyStrategy {

  private static final Duration WARNING_INTERVAL = Duration.ofMinutes(1);

  protected final Logger log = LogManager.getLogger(getClass());
  protected final ScheduledExecutorService scheduler;

  private volatile long lastWarning;

  BackgroundStrategy(String name) {
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              var thread = new Thread(runnable, "zanzibar-consistency-" + name);
              thread.setDaemon(true);
              return thread;
            });
  }

  protected void warn(String message, Throwable error) {
    var now = System.currentTimeMillis();
    if (now - lastWarning >= WARNING_INTERVAL.toMillis()) {
      lastWarning = now;
      log.warn(message, error);
    }
  }

  /** Stops the thread, waiting up to 5 s for a run in progress. */
  @Override
  public void close() {
    scheduler.shutdownNow();
    try {
      scheduler.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
