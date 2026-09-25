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
import static com.netflix.spinnaker.fiat.model.resources.ResourceType.ACCOUNT;
import static com.netflix.spinnaker.fiat.model.resources.ResourceType.APPLICATION;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.config.AccountManagerConfig;
import com.netflix.spinnaker.fiat.config.FiatAdminConfig;
import com.netflix.spinnaker.fiat.model.resources.Account;
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.BuildService;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.permissions.DefaultFallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.zanzibar.resource.AccountResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ApplicationResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.BuildServiceResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end against a real store (both engines): reconcile resources and memberships, then read
 * permissions through {@link ZanzibarPermissionsRepository} as Fiat's controllers do. Requires
 * Docker.
 */
@Testcontainers
class ZanzibarAuthorizationEndToEndTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<ZanzibarResourceType> TYPES =
      List.of(
          new AccountResourceType(Account::getPermissions, null, MAPPER),
          new ApplicationResourceType(
              Application::getPermissions,
              null,
              MAPPER,
              new DefaultFallbackPermissionsResolver(EXECUTE, READ),
              false),
          new BuildServiceResourceType(BuildService::getPermissions, null, MAPPER));

  private static final String SPICEDB_KEY = "zanzibar-e2e-key";

  @Container
  static final GenericContainer<?> SPICEDB =
      new GenericContainer<>("authzed/spicedb:latest")
          .withCommand("serve", "--grpc-preshared-key", SPICEDB_KEY)
          .withExposedPorts(50051)
          .waitingFor(Wait.forListeningPort());

  @Container
  static final GenericContainer<?> OPENFGA =
      new GenericContainer<>("openfga/openfga:latest")
          .withCommand("run", "--http-addr", "0.0.0.0:9190")
          .withExposedPorts(9190)
          .waitingFor(Wait.forHttp("/healthz").forPort(9190).forStatusCode(200));

  static Stream<ZanzibarStore> stores() {
    return Stream.of(
        new SpiceDbZanzibarStore(
            ZanzibarTypes.schema(TYPES),
            SPICEDB.getHost(),
            SPICEDB.getMappedPort(50051),
            SPICEDB_KEY),
        new OpenFgaZanzibarStore(
            ZanzibarTypes.schema(TYPES),
            "http://" + OPENFGA.getHost() + ":" + OPENFGA.getMappedPort(9190)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void reconcileThenReadThroughFiat(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    reconciler.reconcileResource(
        APPLICATION.getName(),
        "foo",
        ZanzibarTypes.grants(new Permissions.Builder().add(READ, "eng").build()));
    reconciler.reconcileResource(
        APPLICATION.getName(),
        "bar",
        ZanzibarTypes.grants(new Permissions.Builder().add(READ, "ops").build()));

    var adminConfig = new FiatAdminConfig();
    adminConfig.getAdmin().setRoles(List.of("admins"));
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            reconciler,
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            adminConfig,
            new AccountManagerConfig(),
            null);
    // Memberships are written the way Fiat stores a login.
    repository.put(repository.get("alice").orElseThrow().setRoles(Set.of(role("eng"))));
    repository.put(repository.get("root").orElseThrow().setRoles(Set.of(role("admins"))));

    var alice = repository.get("alice").orElseThrow().getView();
    assertThat(alice.getApplications())
        .singleElement()
        .satisfies(
            app -> {
              assertThat(app.getName()).isEqualTo("foo");
              assertThat(app.getAuthorizations()).containsExactly(READ);
            });
    assertThat(alice.getRoles()).extracting("name").containsExactly("eng");
    assertThat(alice.isAdmin()).isFalse();

    assertThat(repository.get("bob").orElseThrow().getApplications()).isEmpty();
    assertThat(repository.get("root").orElseThrow().isAdmin()).isTrue();
    assertThat(repository.isEmpty()).isFalse();

    repository.close();
    store.close();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void realIdentifierShapesRoundTrip(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    var urnGroup = "urn:example:group:7654321";
    var urnPerson = "urn:example:person:1234567";
    var app =
        ZanzibarTypes.grants(
            new Permissions.Builder().add(READ, "1234567").add(READ, urnGroup).build());
    var account =
        ZanzibarTypes.grants(new Permissions.Builder().add(READ, "svc@example.com").build());

    reconciler.reconcileResource(APPLICATION.getName(), "my.app", app);
    reconciler.reconcileResource(ACCOUNT.getName(), "prod.us-west-2", account);
    reconciler.reconcileUserMemberships("jane.doe@example.com", Set.of("1234567"));
    reconciler.reconcileUserMemberships(urnPerson, Set.of(urnGroup));
    // X509 logins use the email as a role.
    reconciler.reconcileUserMemberships("svc@example.com", Set.of("svc@example.com"));

    // Ids read back decoded, so reconciling the same state again changes nothing.
    assertThat(reconciler.reconcileResource(APPLICATION.getName(), "my.app", app)).isZero();
    assertThat(reconciler.reconcileResource(ACCOUNT.getName(), "prod.us-west-2", account)).isZero();
    assertThat(reconciler.reconcileUserMemberships(urnPerson, Set.of(urnGroup))).isZero();
    assertThat(store.groupsOf(urnPerson)).containsExactly(urnGroup);
    assertThat(store.isEmpty()).isFalse();

    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            reconciler,
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            null);

    for (var user : List.of("Jane.Doe@Example.com", urnPerson)) {
      assertThat(repository.get(user).orElseThrow().getApplications())
          .extracting("name")
          .containsExactly("my.app");
    }
    assertThat(repository.get("svc@example.com").orElseThrow().getAccounts())
        .extracting("name")
        .containsExactly("prod.us-west-2");
    assertThat(repository.get("jane.doe@example.com").orElseThrow().getAccounts()).isEmpty();

    repository.close();
    store.close();
  }

  private static com.netflix.spinnaker.fiat.model.resources.Role role(String name) {
    return new com.netflix.spinnaker.fiat.model.resources.Role(name);
  }
}
