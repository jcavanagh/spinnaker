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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Follows the engine's change stream from the newest token, observing each change's token.
 * Reconnects from the newest token after 1 s, doubling to 30 s, when the stream ends.
 */
class WatchStrategy extends BackgroundStrategy {

  private static final Duration FIRST_RETRY = Duration.ofSeconds(1);
  private static final Duration MAX_RETRY = Duration.ofSeconds(30);

  private final ConsistencySupport store;

  private volatile Duration retry = FIRST_RETRY;
  private volatile Runnable stop;
  private volatile boolean closed;

  WatchStrategy(ConsistencySupport store) {
    super("watch");
    this.store = store;
  }

  @Override
  public void start() {
    scheduler.execute(this::connect);
  }

  private void connect() {
    if (closed) {
      return;
    }
    try {
      stop = store.watch(store.tokens().current().orElse(null), this::observe, this::ended);
    } catch (RuntimeException e) {
      ended(e);
    }
  }

  private void observe(ConsistencyToken token) {
    retry = FIRST_RETRY;
    store.tokens().observe(token);
  }

  private void ended(@Nullable Throwable error) {
    if (closed) {
      return;
    }
    var delay = retry;
    if (error != null) {
      warn("Zanzibar change stream failed; reconnecting in " + delay.toSeconds() + "s", error);
    }
    var next = delay.multipliedBy(2);
    retry = next.compareTo(MAX_RETRY) > 0 ? MAX_RETRY : next;
    try {
      scheduler.schedule(this::connect, delay.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Closed meanwhile.
    }
  }

  @Override
  public void close() {
    closed = true;
    var current = stop;
    if (current != null) {
      current.run();
    }
    super.close();
  }
}
