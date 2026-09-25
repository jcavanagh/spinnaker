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
import java.util.concurrent.TimeUnit;

/**
 * Every poll interval, offers this replica's newest token to {@link SharedConsistencyTokens}, then
 * observes the shared one.
 */
class SharedSqlStrategy extends BackgroundStrategy {

  private final ConsistencySupport store;
  private final SharedConsistencyTokens shared;
  private final Duration pollInterval;

  // The newest order already shared; only the scheduler thread uses it.
  private long offered = Long.MIN_VALUE;

  SharedSqlStrategy(
      ConsistencySupport store, SharedConsistencyTokens shared, Duration pollInterval) {
    super("shared-sql");
    this.store = store;
    this.shared = shared;
    this.pollInterval = pollInterval;
  }

  @Override
  public void start() {
    scheduler.scheduleWithFixedDelay(this::poll, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void poll() {
    try {
      var key = store.consistencyKey();
      var tokens = store.tokens();
      var own = tokens.current();
      if (own.isPresent() && own.get().order() > offered) {
        shared.offer(key, own.get());
        offered = own.get().order();
      }
      var stored = shared.read(key);
      if (stored.isPresent()) {
        tokens.observe(stored.get());
        offered = Math.max(offered, stored.get().order());
      }
    } catch (RuntimeException e) {
      warn("Sharing the Zanzibar consistency token failed", e);
    }
  }
}
