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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.netflix.spinnaker.security.Authorization;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Verifies the heartbeat alternates adding and removing its marker, and retries a failed beat. */
class HeartbeatStrategyTest {

  private final ConsistencySupport store = mock(ConsistencySupport.class);
  private final List<String> calls = new CopyOnWriteArrayList<>();

  @Test
  void alternatesTheMarkerAndRemovesItOnClose() throws Exception {
    var beats = new CountDownLatch(4);
    record(beats, new AtomicBoolean());
    var heartbeat = new HeartbeatStrategy(store, Duration.ofMillis(10));

    heartbeat.start();
    assertThat(beats.await(10, SECONDS)).isTrue();
    heartbeat.close();

    assertThat(calls).startsWith("add", "remove", "add", "remove");
    for (int i = 0; i < calls.size(); i++) {
      assertThat(calls.get(i)).isEqualTo(i % 2 == 0 ? "add" : "remove");
    }
    assertThat(calls).as("no marker left").last().isEqualTo("remove");
  }

  @Test
  void aFailedBeatIsRetried() throws Exception {
    var beats = new CountDownLatch(2);
    var failNext = new AtomicBoolean(true);
    record(beats, failNext);

    try (var heartbeat = new HeartbeatStrategy(store, Duration.ofMillis(10))) {
      heartbeat.start();
      assertThat(beats.await(10, SECONDS)).isTrue();
    }

    assertThat(calls).startsWith("add failed", "add", "remove");
  }

  /** Records each marker write; the first add fails while {@code failNext} is set. */
  private void record(CountDownLatch beats, AtomicBoolean failNext) {
    doAnswer(
            invocation -> {
              if (failNext.getAndSet(false)) {
                calls.add("add failed");
                throw new IllegalStateException("store unavailable");
              }
              calls.add("add");
              beats.countDown();
              return null;
            })
        .when(store)
        .addAcl(
            eq(HeartbeatStrategy.MARKER_TYPE),
            eq(HeartbeatStrategy.MARKER),
            eq(Authorization.READ),
            startsWith("user:"));
    doAnswer(
            invocation -> {
              calls.add("remove");
              beats.countDown();
              return null;
            })
        .when(store)
        .removeAcl(
            eq(HeartbeatStrategy.MARKER_TYPE),
            eq(HeartbeatStrategy.MARKER),
            eq(Authorization.READ),
            startsWith("user:"));
  }
}
