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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Verifies the watch observes each token, reconnects from the newest one, and stops on close. */
class WatchStrategyTest {

  private final ConsistencyTokens tokens = new ConsistencyTokens(local());
  private final ConsistencySupport store = mock(ConsistencySupport.class);
  private final List<ConsistencyToken> froms = new CopyOnWriteArrayList<>();
  private final BlockingQueue<Stream> streams = new LinkedBlockingQueue<>();
  private final AtomicInteger failures = new AtomicInteger();

  @BeforeEach
  void fakeStreams() {
    when(store.tokens()).thenReturn(tokens);
    when(store.watch(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              if (failures.getAndDecrement() > 0) {
                throw new IllegalStateException("store unavailable");
              }
              froms.add(invocation.getArgument(0));
              var stream = new Stream(invocation.getArgument(1), invocation.getArgument(2));
              streams.add(stream);
              return (Runnable) () -> stream.stopped.set(true);
            });
  }

  @Test
  void observesEachChangesToken() throws Exception {
    try (var watch = new WatchStrategy(store)) {
      watch.start();
      var stream = streams.poll(5, SECONDS);

      stream.onToken.accept(ConsistencyToken.of("a", 1));
      stream.onToken.accept(ConsistencyToken.of("b", 2));

      assertThat(froms).containsExactly((ConsistencyToken) null);
      assertThat(tokens.current()).contains(ConsistencyToken.of("b", 2));
    }
  }

  @Test
  void reconnectsFromTheNewestTokenWhenTheStreamEnds() throws Exception {
    try (var watch = new WatchStrategy(store)) {
      watch.start();
      var first = streams.poll(5, SECONDS);
      first.onToken.accept(ConsistencyToken.of("a", 1));

      first.onEnd.accept(new IllegalStateException("stream reset"));

      assertThat(streams.poll(5, SECONDS)).as("reconnected").isNotNull();
      assertThat(froms).containsExactly(null, ConsistencyToken.of("a", 1));
    }
  }

  @Test
  void retriesAFailedConnect() throws Exception {
    failures.set(1);

    try (var watch = new WatchStrategy(store)) {
      watch.start();

      assertThat(streams.poll(5, SECONDS)).as("connected on the retry").isNotNull();
    }
  }

  @Test
  void closeStopsTheStreamWithoutReconnecting() throws Exception {
    var watch = new WatchStrategy(store);
    watch.start();
    var stream = streams.poll(5, SECONDS);

    watch.close();
    stream.onEnd.accept(new IllegalStateException("cancelled"));

    assertThat(stream.stopped).isTrue();
    assertThat(streams.poll(1500, MILLISECONDS)).isNull();
  }

  private static ConsistencyProperties local() {
    var properties = new ConsistencyProperties();
    properties.setStrategy(Strategy.LOCAL);
    return properties;
  }

  /** One fake change stream: the strategy's callbacks, and whether it was stopped. */
  private static final class Stream {
    final Consumer<ConsistencyToken> onToken;
    final Consumer<Throwable> onEnd;
    final AtomicBoolean stopped = new AtomicBoolean();

    Stream(Consumer<ConsistencyToken> onToken, Consumer<Throwable> onEnd) {
      this.onToken = onToken;
      this.onEnd = onEnd;
    }
  }
}
