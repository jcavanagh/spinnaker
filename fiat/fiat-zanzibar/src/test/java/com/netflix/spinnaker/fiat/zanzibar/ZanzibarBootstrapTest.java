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
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.model.resources.Account;
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.BuildService;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Resource;
import com.netflix.spinnaker.fiat.model.resources.Role;
import com.netflix.spinnaker.fiat.permissions.DefaultFallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import com.netflix.spinnaker.fiat.zanzibar.resource.AccountResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ApplicationResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.BuildServiceResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the bootstrap reconciles every stored type from Fiat's providers, deletes orphans,
 * leaves types without a provider alone, and runs under the single-flight sweep lock. Requires
 * Docker.
 */
@Testcontainers
class ZanzibarBootstrapTest {

  private static final String SPICEDB_KEY = "zanzibar-test-key";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ZanzibarSchema SCHEMA =
      new ZanzibarSchema(List.of("account", "application", "build_service"));

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
            SCHEMA, SPICEDB.getHost(), SPICEDB.getMappedPort(50051), SPICEDB_KEY),
        new OpenFgaZanzibarStore(
            SCHEMA, "http://" + OPENFGA.getHost() + ":" + OPENFGA.getMappedPort(9190)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void sweepReconcilesAllTypesAndDeletesOrphans(ZanzibarStore store) {
    store.applySchema();
    var applications = new InMemoryProvider<Application>();
    var application = new Application();
    application.setName("sw-app");
    application.setPermissions(new Permissions.Builder().add(READ, "sw-eng").build());
    applications.put(application);
    var accounts = new InMemoryProvider<Account>();
    var account = new Account();
    account.setName("sw-acct");
    account.setPermissions(new Permissions.Builder().add(READ, "sw-eng").build());
    accounts.put(account);
    var types =
        List.<ZanzibarResourceType>of(
            new ApplicationResourceType(
                Application::getPermissions,
                applications,
                MAPPER,
                new DefaultFallbackPermissionsResolver(EXECUTE, READ),
                false),
            new AccountResourceType(Account::getPermissions, accounts, MAPPER));

    var reconciler = new Reconciler(store);
    reconciler.reconcileUserMemberships("sw-alice", Set.of("sw-eng"));
    var lock = new RecordingLock();
    var bootstrap =
        new ZanzibarBootstrap(
            store, lock, types, new ServiceAccountMembershipReconciler(null, reconciler, lock));

    bootstrap.run(1000);

    // Ran under the single global sweep lock — one leader across instances.
    assertThat(lock.keys).contains(ZanzibarBootstrap.SWEEP_LOCK_KEY);
    assertThat(store.check("sw-alice", "application", "sw-app", ZanzibarTypes.action(READ)))
        .isTrue();
    assertThat(store.check("sw-alice", "account", "sw-acct", ZanzibarTypes.action(READ))).isTrue();

    // Upstream: the application is deleted.
    applications.remove("sw-app");
    bootstrap.run(1000);

    assertThat(store.read("application", "sw-app")).as("orphan deleted").isEmpty();
    assertThat(store.check("sw-alice", "account", "sw-acct", ZanzibarTypes.action(READ)))
        .as("listed resources stay")
        .isTrue();
    store.close();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void typesWithoutAProviderKeepTheirStoredAcls(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    reconciler.reconcileResource(
        "build_service",
        "jenkins",
        ZanzibarTypes.grants(new Permissions.Builder().add(READ, "np-eng").build()));
    var accounts = new InMemoryProvider<Account>();
    var account = new Account();
    account.setName("np-acct");
    account.setPermissions(new Permissions.Builder().add(READ, "np-eng").build());
    accounts.put(account);
    var types =
        List.<ZanzibarResourceType>of(
            new BuildServiceResourceType(BuildService::getPermissions, null, MAPPER),
            new AccountResourceType(Account::getPermissions, accounts, MAPPER));

    reconciler.reconcileUserMemberships("np-alice", Set.of("np-eng"));
    var lock = new InProcessReconcileLock();
    var bootstrap =
        new ZanzibarBootstrap(
            store, lock, types, new ServiceAccountMembershipReconciler(null, reconciler, lock));

    bootstrap.run(1000);

    assertThat(store.check("np-alice", "account", "np-acct", ZanzibarTypes.action(READ))).isTrue();
    assertThat(store.read("build_service", "jenkins")).as("stored ACL kept").isNotEmpty();
    store.close();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void runReportsTheTuplesItChanged(ZanzibarStore store) {
    store.applySchema();
    var applications = new InMemoryProvider<Application>();
    var application = new Application();
    application.setName("rc-app");
    application.setPermissions(new Permissions.Builder().add(READ, "rc-eng").build());
    applications.put(application);
    var types =
        List.<ZanzibarResourceType>of(
            new ApplicationResourceType(
                Application::getPermissions,
                applications,
                MAPPER,
                new DefaultFallbackPermissionsResolver(EXECUTE, READ),
                false));
    var lock = new InProcessReconcileLock();
    var bootstrap =
        new ZanzibarBootstrap(
            store,
            lock,
            types,
            new ServiceAccountMembershipReconciler(null, new Reconciler(store), lock));

    var first = bootstrap.run(1000).orElseThrow();
    var second = bootstrap.run(1000).orElseThrow();

    assertThat(first.get("resourceTuplesChanged")).isPositive();
    assertThat(first.get("serviceAccountTuplesChanged")).isZero();
    assertThat(second.get("resourceTuplesChanged")).as("nothing left to change").isZero();
    store.close();
  }

  /** A provider over a mutable map, without Fiat's listing cache. */
  private static final class InMemoryProvider<R extends Resource> implements ResourceProvider<R> {
    private final Map<String, R> resources = new ConcurrentHashMap<>();

    void put(R resource) {
      resources.put(resource.getName(), resource);
    }

    void remove(String name) {
      resources.remove(name);
    }

    @Override
    public Set<R> getAll() {
      return Set.copyOf(resources.values());
    }

    @Override
    public Set<R> getAllRestricted(String userId, Set<Role> userRoles, boolean isAdmin) {
      return Set.of();
    }

    @Override
    public Set<R> getAllUnrestricted() {
      return Set.of();
    }

    @Override
    public void clearCache() {}
  }

  /** Records the keys reconciliation locks on, delegating the actual locking in-process. */
  private static final class RecordingLock implements ReconcileLock {
    final List<String> keys = new CopyOnWriteArrayList<>();
    private final ReconcileLock delegate = new InProcessReconcileLock();

    @Override
    public void runExclusively(String key, Runnable action) {
      keys.add(key);
      delegate.runExclusively(key, action);
    }
  }
}
