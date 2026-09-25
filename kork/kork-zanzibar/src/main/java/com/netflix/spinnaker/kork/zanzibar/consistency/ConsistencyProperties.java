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

import java.time.Duration;
import lombok.Data;

/** How a store's replicas share their newest consistency token. */
@Data
public class ConsistencyProperties {

  public enum Strategy {
    /** No tokens. */
    NONE,
    /** This replica's own writes only. */
    LOCAL,
    /** A marker write every {@code heartbeatInterval}. */
    HEARTBEAT,
    /** The engine's change stream. */
    WATCH,
    /** A shared SQL row, polled every {@code pollInterval}. */
    SHARED_SQL
  }

  private Strategy strategy = Strategy.NONE;

  private Duration heartbeatInterval = Duration.ofSeconds(5);

  private Duration pollInterval = Duration.ofSeconds(1);

  /** A token received longer ago is dropped, and reads go without one. */
  private Duration maxAge = Duration.ofMinutes(1);
}
