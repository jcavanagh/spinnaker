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

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/** Verifies the tracker keeps the newest token and drops one older than its max age. */
class ConsistencyTokensTest {

  private final MovableClock clock = new MovableClock();

  @Test
  void hasNoTokenUntilOneIsObserved() {
    assertThat(tokens(Strategy.LOCAL).current()).isEmpty();
  }

  @Test
  void keepsTheNewerToken() {
    var tokens = tokens(Strategy.LOCAL);

    tokens.observe(ConsistencyToken.of("b", 2));
    tokens.observe(ConsistencyToken.of("a", 1));
    assertThat(tokens.current()).contains(ConsistencyToken.of("b", 2));

    tokens.observe(ConsistencyToken.of("c", 3));
    assertThat(tokens.current()).contains(ConsistencyToken.of("c", 3));
  }

  @Test
  void dropsATokenOlderThanMaxAge() {
    var tokens = tokens(Strategy.LOCAL);
    tokens.observe(ConsistencyToken.of("a", 1));

    clock.advance(Duration.ofMinutes(1));
    assertThat(tokens.current()).as("at max age").isPresent();

    clock.advance(Duration.ofMillis(1));
    assertThat(tokens.current()).isEmpty();
  }

  @Test
  void theSameTokenAgainDoesNotRenewItsAge() {
    var tokens = tokens(Strategy.LOCAL);
    tokens.observe(ConsistencyToken.of("a", 1));

    clock.advance(Duration.ofSeconds(59));
    tokens.observe(ConsistencyToken.of("a", 1));
    clock.advance(Duration.ofSeconds(2));

    assertThat(tokens.current()).isEmpty();
  }

  @Test
  void aNewerTokenReplacesAnExpiredOne() {
    var tokens = tokens(Strategy.LOCAL);
    tokens.observe(ConsistencyToken.of("a", 1));
    clock.advance(Duration.ofMinutes(2));

    tokens.observe(ConsistencyToken.of("b", 2));

    assertThat(tokens.current()).contains(ConsistencyToken.of("b", 2));
  }

  @Test
  void noneKeepsNoTokens() {
    var tokens = tokens(Strategy.NONE);

    tokens.observe(ConsistencyToken.of("a", 1));

    assertThat(tokens.current()).isEmpty();
  }

  private ConsistencyTokens tokens(Strategy strategy) {
    var properties = new ConsistencyProperties();
    properties.setStrategy(strategy);
    return new ConsistencyTokens(properties, clock);
  }

  /** A clock the test moves by hand. */
  private static final class MovableClock extends Clock {
    private Instant now = Instant.parse("2026-10-02T00:00:00Z");

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
