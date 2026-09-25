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

import static com.netflix.spinnaker.fiat.model.Authorization.EXECUTE;
import static com.netflix.spinnaker.fiat.model.Authorization.READ;
import static com.netflix.spinnaker.fiat.model.Authorization.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.model.resources.Account;
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.BuildService;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import com.netflix.spinnaker.fiat.permissions.DefaultFallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.providers.ResourcePermissionProvider;
import com.netflix.spinnaker.fiat.zanzibar.resource.AccountResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ApplicationResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.BuildServiceResourceType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Runs without Docker: event payloads derive the permissions Fiat's providers would. */
class ZanzibarResourcePermissionsTest {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final Permissions FROM_PROVIDER =
      new Permissions.Builder().add(READ, "from-provider").build();

  @Test
  void unrestrictedApplicationsAreOmittedWhenUnknownAppsAreOpen() {
    var permissions = permissions(true);

    assertThat(permissions.effective(ResourceType.APPLICATION, Map.of("name", "APP1"))).isEmpty();
  }

  @Test
  void unrestrictedApplicationsAreKeptWhenUnknownAppsAreClosed() {
    var permissions = permissions(false);

    var derived = permissions.effective(ResourceType.APPLICATION, Map.of("name", "APP1"));

    assertThat(derived).hasValueSatisfying(p -> assertThat(p.isRestricted()).isFalse());
  }

  @Test
  void restrictedApplicationsGetTheExecuteFallback() {
    var permissions = permissions(true);
    var application =
        Map.<String, Object>of(
            "name", "APP1",
            "email", "owner@example.com",
            "cloudProviders", "kubernetes",
            "permissions", Map.of("READ", List.of("eng"), "WRITE", List.of("ops")));

    var derived = permissions.effective(ResourceType.APPLICATION, application).orElseThrow();

    assertThat(derived.get(READ)).containsExactly("eng");
    assertThat(derived.get(WRITE)).containsExactly("ops");
    assertThat(derived.get(EXECUTE)).containsExactly("eng");
  }

  @Test
  void accountLikeResourcesUseTheirPermissionProviders() {
    var permissions = permissions(false);
    var resource =
        Map.<String, Object>of(
            "name", "prod.us-west-2", "permissions", Map.of("READ", List.of("eng")));

    for (var type : List.of(ResourceType.ACCOUNT, ResourceType.BUILD_SERVICE)) {
      assertThat(permissions.effective(type, resource)).as(type.getName()).contains(FROM_PROVIDER);
    }
  }

  @Test
  void serviceAccountsConvertFromFront50Json() {
    var permissions = permissions(false);

    var serviceAccount =
        permissions.serviceAccount(
            Map.of(
                "name",
                "svc@managed-service-account",
                "memberOf",
                List.of("eng", "ops"),
                "lastModifiedBy",
                "someone"));

    assertThat(serviceAccount.getName()).isEqualTo("svc@managed-service-account");
    assertThat(serviceAccount.getMemberOf()).containsExactlyInAnyOrder("eng", "ops");
  }

  @Test
  void unsupportedTypesAreRejected() {
    var permissions = permissions(false);

    assertThatThrownBy(() -> permissions.effective(ResourceType.ROLE, Map.of("name", "eng")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static ZanzibarResourcePermissions permissions(boolean allowAccessToUnknownApplications) {
    ResourcePermissionProvider<Application> applications = Application::getPermissions;
    ResourcePermissionProvider<Account> accounts = account -> FROM_PROVIDER;
    ResourcePermissionProvider<BuildService> buildServices = buildService -> FROM_PROVIDER;
    return new ZanzibarResourcePermissions(
        MAPPER,
        List.of(
            new AccountResourceType(accounts, null, MAPPER),
            new ApplicationResourceType(
                applications,
                null,
                MAPPER,
                new DefaultFallbackPermissionsResolver(EXECUTE, READ),
                allowAccessToUnknownApplications),
            new BuildServiceResourceType(buildServices, null, MAPPER)));
  }
}
