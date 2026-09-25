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

import static com.netflix.spinnaker.kork.zanzibar.TestTypes.APPLICATION;
import static com.netflix.spinnaker.security.Authorization.READ;
import static com.netflix.spinnaker.security.Authorization.WRITE;
import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the reconcile path against both engines: updating a resource's permissions revokes the
 * grants it dropped, deleting a resource removes every grant, restricted/unrestricted transitions
 * flip world access, group membership changes revoke access, and reconcile is idempotent. Requires
 * Docker.
 */
@Testcontainers
class ReconciliationTest {

  private static final String SPICEDB_KEY = "zanzibar-test-key";

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
            TestTypes.SCHEMA, SPICEDB.getHost(), SPICEDB.getMappedPort(50051), SPICEDB_KEY),
        new OpenFgaZanzibarStore(
            TestTypes.SCHEMA, "http://" + OPENFGA.getHost() + ":" + OPENFGA.getMappedPort(9190)));
  }

  /**
   * Changing a resource's permissions must revoke the grants it no longer has (the over-grant bug).
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void updateShrinksAccess(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    store.addMember("u-eng", "user:u-alice");
    store.addMember("u-ops", "user:u-dave");

    reconciler.reconcileResource(APPLICATION, "u-app", Map.of(READ, Set.of("u-eng")));
    assertThat(store.check("u-alice", APPLICATION, "u-app", READ)).isTrue();
    assertThat(store.check("u-dave", APPLICATION, "u-app", READ)).isFalse();

    // Permissions change: READ now belongs to ops, not eng.
    reconciler.reconcileResource(APPLICATION, "u-app", Map.of(READ, Set.of("u-ops")));
    assertThat(store.check("u-alice", APPLICATION, "u-app", READ))
        .as("eng grant revoked")
        .isFalse();
    assertThat(store.check("u-dave", APPLICATION, "u-app", READ)).as("ops grant added").isTrue();
    store.close();
  }

  /** Deleting a resource removes every ACL for it. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void deleteRemovesAllAccess(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    store.addMember("d-eng", "user:d-alice");
    store.addMember("d-ops", "user:d-dave");
    reconciler.reconcileResource(
        APPLICATION, "d-app", Map.of(READ, Set.of("d-eng"), WRITE, Set.of("d-ops")));
    assertThat(store.check("d-alice", APPLICATION, "d-app", READ)).isTrue();
    assertThat(store.check("d-dave", APPLICATION, "d-app", WRITE)).isTrue();

    reconciler.deleteResource(APPLICATION, "d-app");
    assertThat(store.read("application", "d-app")).isEmpty();
    assertThat(store.check("d-alice", APPLICATION, "d-app", READ)).isFalse();
    assertThat(store.check("d-dave", APPLICATION, "d-app", WRITE)).isFalse();
    store.close();
  }

  /** Making a restricted resource unrestricted opens it to everyone, on every action. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void restrictedToUnrestricted(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    store.addMember("r-eng", "user:r-alice");
    reconciler.reconcileResource(APPLICATION, "r-app", Map.of(READ, Set.of("r-eng")));
    assertThat(store.check("r-carol", APPLICATION, "r-app", READ)).isFalse();

    reconciler.reconcileResource(APPLICATION, "r-app", Map.of());
    assertThat(store.check("r-carol", APPLICATION, "r-app", READ)).isTrue();
    assertThat(store.check("r-carol", APPLICATION, "r-app", WRITE)).isTrue();
    store.close();
  }

  /** Restricting a previously world-accessible resource removes the wildcard grant. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void unrestrictedToRestricted(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    store.addMember("w-eng", "user:w-alice");
    reconciler.reconcileResource(APPLICATION, "w-app", Map.of());
    assertThat(store.check("w-carol", APPLICATION, "w-app", READ)).isTrue();

    reconciler.reconcileResource(APPLICATION, "w-app", Map.of(READ, Set.of("w-eng")));
    assertThat(store.check("w-carol", APPLICATION, "w-app", READ)).as("wildcard removed").isFalse();
    assertThat(store.check("w-alice", APPLICATION, "w-app", READ)).isTrue();
    store.close();
  }

  /** Removing a user from a group revokes the access they held through it. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void reconcileGroupRemovesMember(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    reconciler.reconcileGroup("g-eng", Set.of("user:g-alice", "user:g-bob"));
    store.addAcl(APPLICATION, "g-app", READ, "group:g-eng");
    assertThat(store.check("g-alice", APPLICATION, "g-app", READ)).isTrue();
    assertThat(store.check("g-bob", APPLICATION, "g-app", READ)).isTrue();

    reconciler.reconcileGroup("g-eng", Set.of("user:g-bob"));
    assertThat(store.check("g-alice", APPLICATION, "g-app", READ)).as("alice removed").isFalse();
    assertThat(store.check("g-bob", APPLICATION, "g-app", READ)).isTrue();
    store.close();
  }

  /** Reconciling to the same desired state is a no-op. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void reconcileIsIdempotent(ZanzibarStore store) {
    store.applySchema();
    var reconciler = new Reconciler(store);
    var permissions = Map.of(READ, Set.of("i-eng"), WRITE, Set.of("i-ops"));

    long first = reconciler.reconcileResource(APPLICATION, "i-app", permissions);
    long second = reconciler.reconcileResource(APPLICATION, "i-app", permissions);
    assertThat(first).isGreaterThan(0);
    assertThat(second).as("second reconcile changes nothing").isZero();
    store.close();
  }
}
