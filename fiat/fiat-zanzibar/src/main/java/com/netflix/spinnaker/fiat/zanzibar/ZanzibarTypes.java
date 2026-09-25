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

import com.netflix.spinnaker.fiat.model.Authorization;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts between Fiat's model and the store's resource types and grants. */
final class ZanzibarTypes {

  /** The schema for the stored types. */
  static ZanzibarSchema schema(List<ZanzibarResourceType> types) {
    return new ZanzibarSchema(types.stream().map(type -> type.type().getName()).toList());
  }

  private ZanzibarTypes() {}

  static Map<com.netflix.spinnaker.security.Authorization, Set<String>> grants(
      Permissions permissions) {
    var grants =
        new EnumMap<com.netflix.spinnaker.security.Authorization, Set<String>>(
            com.netflix.spinnaker.security.Authorization.class);
    permissions.unpack().forEach((action, roles) -> grants.put(action(action), roles));
    return grants;
  }

  static com.netflix.spinnaker.security.Authorization action(Authorization authorization) {
    return com.netflix.spinnaker.security.Authorization.valueOf(authorization.name());
  }
}
