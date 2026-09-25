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

package com.netflix.spinnaker.fiat.zanzibar.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties.Strategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyStrategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import com.netflix.spinnaker.kork.zanzibar.consistency.SharedConsistencyTokens;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Verifies an engine's consistency strategy is checked against its store and Fiat's database. */
class ZanzibarEngineTest {

  private final ConsistencySupport store = mock(ConsistencySupport.class);

  @Test
  void anEngineWithoutTokensSharesNothing() {
    ZanzibarEngine engine = schema -> store;

    assertThat(engine.consistencyStrategy(store, null)).isSameAs(ConsistencyStrategy.NONE);
  }

  @Test
  void sharedSqlWithoutFiatsDatabaseFailsNamingTheSetting() {
    assertThatThrownBy(
            () ->
                ZanzibarEngine.strategy(
                    "fiat.zanzibar.spicedb.consistency",
                    properties(Strategy.SHARED_SQL),
                    store,
                    null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "fiat.zanzibar.spicedb.consistency.strategy=shared-sql needs Fiat's SQL database"
                + " (sql.enabled=true)");
  }

  @Test
  void aStoreWithoutTokensFails() {
    var plain = mock(ZanzibarStore.class);
    when(plain.consistency()).thenReturn(Optional.empty());
    when(plain.providerId()).thenReturn("openfga");

    assertThatThrownBy(
            () ->
                ZanzibarEngine.strategy(
                    "fiat.zanzibar.openfga.consistency",
                    properties(Strategy.HEARTBEAT),
                    plain,
                    null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("openfga has no consistency tokens");
  }

  @Test
  void buildsTheConfiguredStrategy() {
    when(store.consistency()).thenReturn(Optional.of(store));

    try (var strategy =
        ZanzibarEngine.strategy(
            "fiat.zanzibar.spicedb.consistency",
            properties(Strategy.SHARED_SQL),
            store,
            mock(SharedConsistencyTokens.class))) {
      assertThat(strategy).isNotSameAs(ConsistencyStrategy.NONE);
    }
  }

  private static ConsistencyProperties properties(Strategy strategy) {
    var properties = new ConsistencyProperties();
    properties.setStrategy(strategy);
    return properties;
  }
}
