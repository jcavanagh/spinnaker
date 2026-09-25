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

package com.netflix.spinnaker.kork.zanzibar;

import com.netflix.spinnaker.security.Authorization;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import lombok.Getter;

/**
 * The authorization model: the resource types to define, each with one relation per {@link
 * Authorization} that accepts a user, the {@code user:*} wildcard (world-access), or a group's
 * members.
 */
public final class ZanzibarSchema {

  /** The resource types to define, e.g. {@code application}. */
  @Getter private final List<String> resourceTypes;

  public ZanzibarSchema(Collection<String> resourceTypes) {
    this.resourceTypes = List.copyOf(resourceTypes);
  }

  /** Lowercase {@link Authorization} names: the per-action relation names. */
  public static List<String> actions() {
    return Arrays.stream(Authorization.values())
        .map(a -> a.name().toLowerCase(Locale.ROOT))
        .toList();
  }
}
