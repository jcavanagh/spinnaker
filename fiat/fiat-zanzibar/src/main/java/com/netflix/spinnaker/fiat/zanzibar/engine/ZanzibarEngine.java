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

import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyProperties;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyStrategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.SharedConsistencyTokens;
import javax.annotation.Nullable;

/** Builds the store adapter for the engine {@code fiat.zanzibar.engine} names. */
@FunctionalInterface
public interface ZanzibarEngine {
  ZanzibarStore create(ZanzibarSchema schema);

  /** How replicas share {@code store}'s consistency token; none unless the engine has tokens. */
  default ConsistencyStrategy consistencyStrategy(
      ZanzibarStore store, @Nullable SharedConsistencyTokens shared) {
    return ConsistencyStrategy.NONE;
  }

  /**
   * The strategy {@code properties} names for {@code store}. {@code prefix} names the settings in
   * errors.
   */
  static ConsistencyStrategy strategy(
      String prefix,
      ConsistencyProperties properties,
      ZanzibarStore store,
      @Nullable SharedConsistencyTokens shared) {
    if (properties.getStrategy() == ConsistencyProperties.Strategy.SHARED_SQL && shared == null) {
      throw new IllegalStateException(
          prefix + ".strategy=shared-sql needs Fiat's SQL database (sql.enabled=true)");
    }
    var support =
        store
            .consistency()
            .orElseThrow(
                () -> new IllegalStateException(store.providerId() + " has no consistency tokens"));
    return ConsistencyStrategy.create(properties, support, shared);
  }
}
