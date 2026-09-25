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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Derives a resource's effective permissions exactly as Fiat's resource providers do. */
public class ZanzibarResourcePermissions {

  private final ObjectMapper objectMapper;
  private final Map<ResourceType, ZanzibarResourceType> types = new HashMap<>();

  public ZanzibarResourcePermissions(ObjectMapper objectMapper, List<ZanzibarResourceType> types) {
    this.objectMapper = objectMapper;
    types.forEach(type -> this.types.put(type.type(), type));
  }

  /** The resource's effective permissions, or empty when Fiat's providers would omit it. */
  public Optional<Permissions> effective(ResourceType type, Map<String, Object> resource) {
    var zanzibarType = types.get(type);
    if (zanzibarType == null) {
      throw new IllegalArgumentException("Unsupported resource type: " + type.getName());
    }
    return zanzibarType.effective(resource);
  }

  public ServiceAccount serviceAccount(Map<String, Object> resource) {
    return objectMapper.convertValue(resource, ServiceAccount.class);
  }
}
