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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Verifies each configured strategy name builds its strategy. */
class ConsistencyStrategyTest {

  private final ConsistencySupport store = mock(ConsistencySupport.class);

  @ParameterizedTest
  @EnumSource(
      value = Strategy.class,
      names = {"NONE", "LOCAL"})
  void noneAndLocalShareNothing(Strategy strategy) {
    assertThat(ConsistencyStrategy.create(properties(strategy), store, null))
        .isSameAs(ConsistencyStrategy.NONE);
  }

  @Test
  void eachSharingStrategyIsBuilt() {
    try (var heartbeat = ConsistencyStrategy.create(properties(Strategy.HEARTBEAT), store, null);
        var watch = ConsistencyStrategy.create(properties(Strategy.WATCH), store, null);
        var sharedSql =
            ConsistencyStrategy.create(
                properties(Strategy.SHARED_SQL), store, mock(SharedConsistencyTokens.class))) {
      assertThat(heartbeat).isInstanceOf(HeartbeatStrategy.class);
      assertThat(watch).isInstanceOf(WatchStrategy.class);
      assertThat(sharedSql).isInstanceOf(SharedSqlStrategy.class);
    }
  }

  @Test
  void sharedSqlNeedsSharedTokens() {
    assertThatThrownBy(
            () -> ConsistencyStrategy.create(properties(Strategy.SHARED_SQL), store, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SharedConsistencyTokens");
  }

  private static ConsistencyProperties properties(Strategy strategy) {
    var properties = new ConsistencyProperties();
    properties.setStrategy(strategy);
    return properties;
  }
}
