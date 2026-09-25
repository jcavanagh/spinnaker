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

import com.netflix.spinnaker.fiat.config.UnrestrictedResourceConfig;
import com.netflix.spinnaker.security.SpinnakerUsers;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Normalizes identifiers as stock Fiat does, so store keys match its lowercased ACLs and checks.
 */
final class ZanzibarPrincipals {

  private ZanzibarPrincipals() {}

  /**
   * As Fiat's {@code ControllerSupport.convert}: lowercase, with anonymous as the unrestricted
   * user.
   */
  static String userId(String userId) {
    if (SpinnakerUsers.ANONYMOUS.equalsIgnoreCase(userId)) {
      return UnrestrictedResourceConfig.UNRESTRICTED_USERNAME;
    }
    return userId.toLowerCase();
  }

  /** As {@code Permissions.Builder}: trimmed, lowercased, blanks dropped. */
  static Set<String> roles(Collection<String> roles) {
    var normalized = new LinkedHashSet<String>();
    for (var role : roles) {
      var trimmed = role.trim();
      if (!trimmed.isEmpty()) {
        normalized.add(trimmed.toLowerCase());
      }
    }
    return normalized;
  }
}
