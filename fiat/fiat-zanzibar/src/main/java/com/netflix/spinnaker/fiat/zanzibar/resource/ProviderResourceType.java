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

package com.netflix.spinnaker.fiat.zanzibar.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Resource;
import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import com.netflix.spinnaker.fiat.providers.ResourcePermissionProvider;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * A type whose permissions come from its {@link ResourcePermissionProvider} and whose resources
 * come from its {@link ResourceProvider}.
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ProviderResourceType<R extends Resource.AccessControlled>
    implements ZanzibarResourceType {

  private final ResourceType type;
  private final Class<R> modelClass;
  private final ResourcePermissionProvider<R> permissionProvider;

  /** Null when the service that owns the type is not configured. */
  private final ResourceProvider<R> resourceProvider;

  private final ObjectMapper objectMapper;

  @Override
  public ResourceType type() {
    return type;
  }

  @Override
  public Optional<Permissions> effective(Map<String, Object> resource) {
    return Optional.of(permissions(resource));
  }

  @Override
  public Optional<Collection<? extends Resource.AccessControlled>> all() {
    if (resourceProvider == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(resourceProvider.getAll());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to list " + type.getName() + " from Fiat provider", e);
    }
  }

  /** The permission provider's answer for a published resource. */
  protected Permissions permissions(Map<String, Object> resource) {
    return permissionProvider.getPermissions(objectMapper.convertValue(resource, modelClass));
  }
}
