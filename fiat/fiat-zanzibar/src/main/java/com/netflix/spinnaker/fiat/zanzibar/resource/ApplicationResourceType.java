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
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Resource;
import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import com.netflix.spinnaker.fiat.permissions.FallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.providers.ResourcePermissionProvider;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import java.util.Map;
import java.util.Optional;

/** Front50 applications, with the execute fallback and the unknown-application rule. */
public class ApplicationResourceType extends ProviderResourceType<Application> {

  private final FallbackPermissionsResolver executeFallback;
  private final boolean allowAccessToUnknownApplications;

  public ApplicationResourceType(
      ResourcePermissionProvider<Application> permissionProvider,
      ResourceProvider<Application> resourceProvider,
      ObjectMapper objectMapper,
      FallbackPermissionsResolver executeFallback,
      boolean allowAccessToUnknownApplications) {
    super(
        ResourceType.APPLICATION,
        Application.class,
        permissionProvider,
        resourceProvider,
        objectMapper);
    this.executeFallback = executeFallback;
    this.allowAccessToUnknownApplications = allowAccessToUnknownApplications;
  }

  @Override
  public Optional<Permissions> effective(Map<String, Object> resource) {
    var permissions = permissions(resource);
    if (executeFallback.shouldResolve(permissions)) {
      permissions = executeFallback.resolve(permissions);
    }
    // As DefaultApplicationResourceProvider: unrestricted apps are omitted when unknown apps are
    // open.
    return allowAccessToUnknownApplications && !permissions.isRestricted()
        ? Optional.empty()
        : Optional.of(permissions);
  }

  @Override
  public Resource newResource(String name, Permissions permissions) {
    var application = new Application();
    application.setName(name);
    application.setPermissions(permissions);
    return application;
  }
}
