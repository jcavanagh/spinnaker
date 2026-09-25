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

package com.netflix.spinnaker.kork.zanzibar;

import static com.netflix.spinnaker.security.Authorization.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Verifies writes take one permit per write request before they run, and reads take none. */
class RateLimitedZanzibarStoreTest {

  private final ZanzibarStore delegate = mock(ZanzibarStore.class);
  private final RateLimiter limiter = mock(RateLimiter.class);
  private final RateLimitedZanzibarStore store = new RateLimitedZanzibarStore(delegate, limiter);

  @BeforeEach
  void requestsOf100() {
    when(delegate.writeBatchSize()).thenReturn(100);
  }

  @Test
  void writeTakesOnePermitPerRequestFirst() {
    var tuples = tuples(250);

    store.write(tuples);

    var order = inOrder(limiter, delegate);
    order.verify(limiter).acquire(3);
    order.verify(delegate).write(tuples);
  }

  @Test
  void deleteTakesOnePermitPerRequestFirst() {
    var tuples = tuples(100);

    store.delete(tuples);

    var order = inOrder(limiter, delegate);
    order.verify(limiter).acquire(1);
    order.verify(delegate).delete(tuples);
  }

  @Test
  void applyCountsWritesAndDeletesApart() {
    var writes = tuples(101);
    var deletes = tuples(1);

    store.apply(writes, deletes);

    var order = inOrder(limiter, delegate);
    order.verify(limiter).acquire(3);
    order.verify(delegate).apply(writes, deletes);
  }

  @Test
  void anEmptyWriteTakesNoPermit() {
    store.write(List.of());

    verify(limiter, never()).acquire(anyInt());
    verify(delegate).write(List.of());
  }

  @Test
  void aStoreWithoutABatchSizeTakesOnePermitPerCall() {
    new RateLimitedZanzibarStore(new FakeZanzibarStore(Map.of()), limiter).write(tuples(5000));

    verify(limiter).acquire(1);
  }

  @Test
  void readsTakeNoPermits() {
    store.check("alice", "application", "app", READ);
    store.isMember("alice", "eng");
    store.groupsOf("alice");
    store.lookupResources("alice", "application", READ);
    store.read("application", "app");
    store.objectIds("application");
    store.isEmpty();

    verifyNoInteractions(limiter);
  }

  @Test
  void consistencyIsTheDelegates() {
    var support = mock(ConsistencySupport.class);
    when(delegate.consistency()).thenReturn(Optional.of(support));

    assertThat(store.consistency()).containsSame(support);
  }

  private static List<ZanzibarRelationship> tuples(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> ZanzibarRelationship.member("g" + i, "user:u"))
        .toList();
  }
}
