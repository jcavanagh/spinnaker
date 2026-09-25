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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * A store's newest consistency token, from its own writes and its {@link ConsistencyStrategy}.
 * Disabled for {@link ConsistencyProperties.Strategy#NONE}.
 */
public class ConsistencyTokens {

  private final boolean enabled;
  private final Duration maxAge;
  private final Clock clock;

  // Written in observe's order, so a reader that sees a new token sees its time.
  private volatile Instant receivedAt;
  private volatile ConsistencyToken token;

  public ConsistencyTokens(ConsistencyProperties properties) {
    this(properties, Clock.systemUTC());
  }

  ConsistencyTokens(ConsistencyProperties properties, Clock clock) {
    this.enabled = properties.getStrategy() != ConsistencyProperties.Strategy.NONE;
    this.maxAge = properties.getMaxAge();
    this.clock = clock;
  }

  /** Keeps {@code next} if it is newer than the current token. */
  public synchronized void observe(ConsistencyToken next) {
    if (enabled && (token == null || next.order() > token.order())) {
      receivedAt = clock.instant();
      token = next;
    }
  }

  /** The newest token, unless none was received within {@code maxAge}. */
  public Optional<ConsistencyToken> current() {
    var current = token;
    if (current == null || receivedAt.plus(maxAge).isBefore(clock.instant())) {
      return Optional.empty();
    }
    return Optional.of(current);
  }
}
