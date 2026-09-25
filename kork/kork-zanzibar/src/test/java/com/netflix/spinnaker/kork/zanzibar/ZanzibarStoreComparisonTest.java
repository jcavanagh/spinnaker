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

import com.netflix.spinnaker.kork.zanzibar.ingest.IngestionPipeline;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the same authorization model against SpiceDB and OpenFGA behind one {@link ZanzibarStore}
 * interface. Scenarios share the two engine containers: the core model, a full-matrix access check
 * across every resource type with a variety of users, a scale ingest, and enumeration. Requires
 * Docker. (Admin bypass and View assembly are exercised in {@code fiat-zanzibar}'s {@code
 * ZanzibarAuthorizationEndToEndTest}, where they live.)
 */
@Testcontainers
class ZanzibarStoreComparisonTest {

  private static final Logger log = LogManager.getLogger(ZanzibarStoreComparisonTest.class);
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

  /** Core properties: identity-only checks, nested groups, wildcard, per-action, revocation. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void demonstratesStoreBackedAuthorization(ZanzibarStore store) {
    store.applySchema();
    store.addMember("eng", "user:alice");
    store.addMember("eng", "group:eng-sub");
    store.addMember("eng-sub", "user:bob");
    store.addAcl(APPLICATION, "foo", READ, "group:eng");
    store.addAcl(APPLICATION, "public", READ, "user:*");

    assertThat(store.check("alice", APPLICATION, "foo", READ)).isTrue();
    assertThat(store.check("bob", APPLICATION, "foo", READ)).isTrue(); // nested
    assertThat(store.check("carol", APPLICATION, "foo", READ)).isFalse();
    assertThat(store.check("carol", APPLICATION, "public", READ)).isTrue(); // wildcard
    assertThat(store.check("alice", APPLICATION, "foo", WRITE)).isFalse(); // per-action

    store.removeMember("eng", "user:alice");
    assertThat(store.check("alice", APPLICATION, "foo", READ)).isFalse(); // revocation
    store.close();
  }

  /**
   * Ingest fixtures via the pipelines, then verify access for a variety of users to every resource
   * type.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void verifiesAccessToEveryResourceType(ZanzibarStore store) {
    store.applySchema();
    var pipeline = new IngestionPipeline(store);
    pipeline.ingestRoles(SyntheticData.fixtureRoles());
    pipeline.ingestResources(SyntheticData.fixtureResources());

    // Admin resolves from membership in the admin group.
    assertThat(store.isMember(SyntheticData.ROOT, SyntheticData.ADMINS)).isTrue();
    assertThat(store.isMember(SyntheticData.ALICE, SyntheticData.ADMINS)).isFalse();

    for (var type : TestTypes.SCHEMA.getResourceTypes()) {
      var t = type;
      var secure = SyntheticData.secure(type);
      var open = SyntheticData.open(type);

      // eng gets READ on the restricted resource (directly and via nested group); nobody else does.
      assertThat(store.check(SyntheticData.ALICE, type, secure, READ))
          .as("%s alice read", t)
          .isTrue();
      assertThat(store.check(SyntheticData.BOB, type, secure, READ)).as("%s bob read", t).isTrue();
      assertThat(store.check(SyntheticData.CAROL, type, secure, READ))
          .as("%s carol read", t)
          .isFalse();
      assertThat(store.check(SyntheticData.DAVE, type, secure, READ))
          .as("%s dave read", t)
          .isFalse();

      // ops gets WRITE; eng does not.
      assertThat(store.check(SyntheticData.DAVE, type, secure, WRITE))
          .as("%s dave write", t)
          .isTrue();
      assertThat(store.check(SyntheticData.ALICE, type, secure, WRITE))
          .as("%s alice write", t)
          .isFalse();

      // The unrestricted instance is readable by everyone.
      assertThat(store.check(SyntheticData.CAROL, type, open, READ)).as("%s open", t).isTrue();
    }
    store.close();
  }

  /**
   * Ingest a large, nested graph and a large resource set; time it and verify a deterministic
   * probe.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void ingestsAtScale(ZanzibarStore store) {
    int groups = Integer.getInteger("zanzibar.scale.groups", 2000);
    int users = Integer.getInteger("zanzibar.scale.users", 500);
    int perType = Integer.getInteger("zanzibar.scale.resourcesPerType", 500);

    store.applySchema();
    var pipeline = new IngestionPipeline(store);

    long start = System.nanoTime();
    long roleRels = pipeline.ingestRoles(SyntheticData.roleMemberships(groups, users, 20, 42));
    long resourceRels =
        pipeline.ingestResources(SyntheticData.resources(perType, 0.1, groups, 5, 7));
    long millis = (System.nanoTime() - start) / 1_000_000;
    log.info(
        "[{}] ingested {} role + {} resource relationships ({} groups, {} users, {}/type) in {} ms",
        store.providerId(),
        roleRels,
        resourceRels,
        groups,
        users,
        perType,
        millis);

    assertThat(roleRels).isGreaterThan(0);
    assertThat(resourceRels).isGreaterThan(0);

    // Deterministic correctness probe at scale.
    store.addMember("g0", "user:probe");
    store.addAcl(APPLICATION, "probe-app", READ, "group:g0");
    assertThat(store.check("probe", APPLICATION, "probe-app", READ)).isTrue();
    assertThat(store.check("stranger", APPLICATION, "probe-app", READ)).isFalse();
    store.close();
  }

  /** Enumeration for UI list/filter: the resources of a type a user may act on (Fiat's View). */
  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void enumeratesAccessibleResources(ZanzibarStore store) {
    store.applySchema();
    store.addMember("en-eng", "user:en-alice");
    store.addAcl(APPLICATION, "en-a", READ, "group:en-eng");
    store.addAcl(APPLICATION, "en-b", READ, "group:en-eng");
    store.addAcl(APPLICATION, "en-c", READ, "user:en-other"); // not alice
    store.addAcl(APPLICATION, "en-open", READ, "user:*"); // world

    assertThat(store.lookupResources("en-alice", APPLICATION, READ))
        .contains("en-a", "en-b", "en-open")
        .doesNotContain("en-c");
    store.close();
  }
}
