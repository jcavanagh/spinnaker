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
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyStrategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.SharedConsistencyTokens;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import javax.annotation.Nullable;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** The SpiceDB engine, the default. */
@AutoConfiguration
@ConditionalOnProperty(prefix = "fiat.zanzibar", name = "enabled", havingValue = "true")
@ConditionalOnProperty(
    prefix = "fiat.zanzibar",
    name = "engine",
    havingValue = "spicedb",
    matchIfMissing = true)
@EnableConfigurationProperties(SpiceDbProperties.class)
public class SpiceDbEngineConfig {

  @Bean
  public ZanzibarEngine spiceDbEngine(SpiceDbProperties properties) {
    return new ZanzibarEngine() {
      @Override
      public ZanzibarStore create(ZanzibarSchema schema) {
        return new SpiceDbZanzibarStore(
            schema,
            properties.getHost(),
            properties.getPort(),
            properties.getPresharedKey(),
            properties.getConsistency());
      }

      @Override
      public ConsistencyStrategy consistencyStrategy(
          ZanzibarStore store, @Nullable SharedConsistencyTokens shared) {
        return ZanzibarEngine.strategy(
            "fiat.zanzibar.spicedb.consistency", properties.getConsistency(), store, shared);
      }
    };
  }
}
