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

import javax.annotation.Nullable;

/** Shares a store's newest consistency token between replicas, through its tokens. */
public interface ConsistencyStrategy extends AutoCloseable {

  /** {@code none} and {@code local}: nothing to share. */
  ConsistencyStrategy NONE =
      new ConsistencyStrategy() {
        @Override
        public void start() {}

        @Override
        public void close() {}
      };

  void start();

  @Override
  void close();

  /** The configured strategy for {@code store}; {@code shared-sql} needs {@code shared}. */
  static ConsistencyStrategy create(
      ConsistencyProperties properties,
      ConsistencySupport store,
      @Nullable SharedConsistencyTokens shared) {
    return switch (properties.getStrategy()) {
      case NONE, LOCAL -> NONE;
      case HEARTBEAT -> new HeartbeatStrategy(store, properties.getHeartbeatInterval());
      case WATCH -> new WatchStrategy(store);
      case SHARED_SQL -> {
        if (shared == null) {
          throw new IllegalStateException("The shared-sql strategy needs SharedConsistencyTokens");
        }
        yield new SharedSqlStrategy(store, shared, properties.getPollInterval());
      }
    };
  }
}
