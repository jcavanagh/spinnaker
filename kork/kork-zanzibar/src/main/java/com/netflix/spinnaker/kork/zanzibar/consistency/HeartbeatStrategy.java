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

import com.netflix.spinnaker.security.Authorization;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Every interval, alternately adds and removes a marker grant for this instance. Each write gives
 * the store a token newer than every write committed before it.
 */
class HeartbeatStrategy extends BackgroundStrategy {

  static final String MARKER_TYPE = "application";
  static final String MARKER = "__zanzibar_consistency";

  private final ConsistencySupport store;
  private final Duration interval;
  private final String subject = "user:" + instanceId();

  // Only the scheduler thread changes it, until close.
  private volatile boolean present;

  HeartbeatStrategy(ConsistencySupport store, Duration interval) {
    super("heartbeat");
    this.store = store;
    this.interval = interval;
  }

  @Override
  public void start() {
    scheduler.scheduleWithFixedDelay(this::beat, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void beat() {
    try {
      if (present) {
        store.removeAcl(MARKER_TYPE, MARKER, Authorization.READ, subject);
      } else {
        store.addAcl(MARKER_TYPE, MARKER, Authorization.READ, subject);
      }
      present = !present;
    } catch (RuntimeException e) {
      warn("Zanzibar consistency heartbeat failed", e);
    }
  }

  @Override
  public void close() {
    super.close();
    if (present) {
      try {
        store.removeAcl(MARKER_TYPE, MARKER, Authorization.READ, subject);
      } catch (RuntimeException e) {
        log.warn("Could not remove the Zanzibar consistency marker for {}", subject, e);
      }
    }
  }

  private static String instanceId() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return UUID.randomUUID().toString();
    }
  }
}
