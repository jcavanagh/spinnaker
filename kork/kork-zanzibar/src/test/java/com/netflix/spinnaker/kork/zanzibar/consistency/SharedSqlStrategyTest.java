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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the shared-SQL strategy offers this replica's newer tokens and observes the shared one.
 */
class SharedSqlStrategyTest {

  private static final String KEY = "titan:test";

  private final ConsistencyTokens tokens = new ConsistencyTokens(local());
  private final ConsistencySupport store = mock(ConsistencySupport.class);
  private final FakeShared shared = new FakeShared();

  @BeforeEach
  void fakeStore() {
    when(store.tokens()).thenReturn(tokens);
    when(store.consistencyKey()).thenReturn(KEY);
  }

  @Test
  void offersThisReplicasTokenOnce() {
    tokens.observe(ConsistencyToken.of("a", 1));

    try (var strategy = start()) {
      eventually(() -> shared.reads.get() >= 5);
      assertThat(shared.read(KEY)).contains(ConsistencyToken.of("a", 1));
      assertThat(shared.offers.get()).isEqualTo(1);

      tokens.observe(ConsistencyToken.of("c", 3));
      eventually(() -> shared.offers.get() == 2);
      assertThat(shared.read(KEY)).contains(ConsistencyToken.of("c", 3));
    }
  }

  @Test
  void observesANewerSharedToken() {
    shared.tokens.put(KEY, ConsistencyToken.of("b", 5));
    tokens.observe(ConsistencyToken.of("a", 1));

    try (var strategy = start()) {
      eventually(() -> tokens.current().equals(Optional.of(ConsistencyToken.of("b", 5))));
    }
  }

  @Test
  void doesNotOfferBackATokenItRead() {
    shared.tokens.put(KEY, ConsistencyToken.of("b", 5));

    try (var strategy = start()) {
      eventually(() -> shared.reads.get() >= 5);
      assertThat(tokens.current()).contains(ConsistencyToken.of("b", 5));
      assertThat(shared.offers.get()).isZero();
    }
  }

  @Test
  void keepsPollingAfterAFailure() {
    shared.failNext.set(true);
    shared.tokens.put(KEY, ConsistencyToken.of("b", 5));

    try (var strategy = start()) {
      eventually(() -> tokens.current().isPresent());
    }
  }

  private SharedSqlStrategy start() {
    var strategy = new SharedSqlStrategy(store, shared, Duration.ofMillis(10));
    strategy.start();
    return strategy;
  }

  private static void eventually(BooleanSupplier condition) {
    var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("condition met within 10 s").isLessThan(deadline);
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private static ConsistencyProperties local() {
    var properties = new ConsistencyProperties();
    properties.setStrategy(Strategy.LOCAL);
    return properties;
  }

  /** Keeps the newest token per key, counting offers and reads; one read fails on request. */
  private static final class FakeShared implements SharedConsistencyTokens {
    final Map<String, ConsistencyToken> tokens = new ConcurrentHashMap<>();
    final AtomicInteger offers = new AtomicInteger();
    final AtomicInteger reads = new AtomicInteger();
    final AtomicBoolean failNext = new AtomicBoolean();

    @Override
    public void offer(String key, ConsistencyToken token) {
      offers.incrementAndGet();
      tokens.merge(key, token, (current, next) -> next.order() > current.order() ? next : current);
    }

    @Override
    public Optional<ConsistencyToken> read(String key) {
      reads.incrementAndGet();
      if (failNext.getAndSet(false)) {
        throw new IllegalStateException("database unavailable");
      }
      return Optional.ofNullable(tokens.get(key));
    }
  }
}
