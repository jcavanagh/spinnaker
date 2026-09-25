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

package com.netflix.spinnaker.fiat.zanzibar;

import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the store-backed Fiat authorization backend. Prefix {@code fiat.zanzibar}. */
@Data
@ConfigurationProperties("fiat.zanzibar")
public class FiatZanzibarProperties {

  /** Master switch; when true the store backend replaces Fiat's materialized repository. */
  private boolean enabled = false;

  /** Selects the engine configuration, e.g. {@code spicedb}. */
  private String engine = "spicedb";

  private Duration cacheTtl = Duration.ofSeconds(10);
  private long cacheMaxSize = 50_000;

  /** Store lookups View builds run concurrently, across all requests on this instance. */
  private int lookupParallelism = 32;
}
