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

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.spinnaker.kork.zanzibar.ingest.IngestionPipeline;
import com.netflix.spinnaker.kork.zanzibar.ingest.ResourceSource;
import com.netflix.spinnaker.kork.zanzibar.ingest.RoleSource;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * Load benchmark for both engines at Fiat's target scale: thousands of users, up to 10k roles each,
 * combined with thousands of resources per type. It ingests a synthetic graph (measuring ingest
 * throughput) then reports latency percentiles for the hot paths a running Fiat drives — point
 * {@code check}, {@code lookupResources} (View enumeration, once per type per login), and {@code
 * isMember} (admin resolution) — against the raw engine adapters (no cache), so the numbers reflect
 * the store itself.
 *
 * <p>Requires Docker and is expensive, so it is gated: it only runs with {@code
 * -Dzanzibar.benchmark=true}. Defaults are modest for a quick sanity run; dial to the real target
 * with the {@code zanzibar.bench.*} properties (forwarded to the test JVM by {@code
 * kork-zanzibar.gradle}):
 *
 * <pre>
 * ./gradlew :kork:kork-zanzibar:test --tests "*ZanzibarStoreBenchmark" \
 *   -Dzanzibar.benchmark=true \
 *   -Dzanzibar.bench.groups=20000 -Dzanzibar.bench.users=2000 \
 *   -Dzanzibar.bench.rolesPerUser=10000 -Dzanzibar.bench.resourcesPerType=3000
 * </pre>
 *
 * <p>Writes are limited to {@code -Dzanzibar.bench.rate} requests per second (1,000).
 *
 * <p>Both engines run on a shared PostgreSQL container by default — representative of production
 * and stable under bulk ingest. Pass {@code -Dzanzibar.bench.datastore=memory} for the in-memory
 * datastore (faster to start, but its MVCC revision retention makes it unsuitable for large
 * ingests).
 */
@EnabledIfSystemProperty(named = "zanzibar.benchmark", matches = "true")
class ZanzibarStoreBenchmark {

  private static final Logger log = LogManager.getLogger(ZanzibarStoreBenchmark.class);
  private static final String SPICEDB_KEY = "zanzibar-bench-key";

  // Scale knobs. "rolesPerUser" is direct group memberships per user; nesting adds transitively.
  private static final int GROUPS = Integer.getInteger("zanzibar.bench.groups", 3000);
  private static final int USERS = Integer.getInteger("zanzibar.bench.users", 200);
  private static final int ROLES_PER_USER = Integer.getInteger("zanzibar.bench.rolesPerUser", 1000);
  private static final int PER_TYPE = Integer.getInteger("zanzibar.bench.resourcesPerType", 500);
  private static final int ROLES_PER_RESOURCE =
      Integer.getInteger("zanzibar.bench.rolesPerResource", 5);
  private static final int CHECK_SAMPLES = Integer.getInteger("zanzibar.bench.checkSamples", 2000);
  private static final int LOOKUP_SAMPLES = Integer.getInteger("zanzibar.bench.lookupSamples", 200);

  /** Ingest buffer size; larger keeps OpenFGA's parallel writer saturated between flushes. */
  private static final int INGEST_FLUSH = Integer.getInteger("zanzibar.bench.flushSize", 5000);

  private static final int WRITE_RATE = Integer.getInteger("zanzibar.bench.rate", 1000);

  /** Ingest progress is logged at most this often, so long runs show they're advancing. */
  private static final long PROGRESS_INTERVAL_MS = 5000;

  /** Backing datastore: {@code postgres} (default) or {@code memory}. */
  private static final boolean POSTGRES =
      !"memory".equalsIgnoreCase(System.getProperty("zanzibar.bench.datastore", "postgres"));

  /** Forward the engine containers' stdout/stderr (crash/panic reasons); off unless set. */
  private static final boolean CONTAINER_LOGS = Boolean.getBoolean("zanzibar.bench.containerLogs");

  private static final Network NETWORK = Network.newNetwork();

  private static GenericContainer<?> postgres;
  private static GenericContainer<?> spicedb;
  private static GenericContainer<?> openfga;

  @BeforeAll
  static void startStores() {
    if (POSTGRES) {
      postgres =
          new GenericContainer<>("postgres:16")
              .withNetwork(NETWORK)
              .withNetworkAliases("postgres")
              .withEnv("POSTGRES_PASSWORD", "postgres")
              .withCopyToContainer(
                  Transferable.of("CREATE DATABASE spicedb;\nCREATE DATABASE openfga;\n"),
                  "/docker-entrypoint-initdb.d/init.sql")
              .withExposedPorts(5432)
              .waitingFor(
                  Wait.forLogMessage(".*database system is ready to accept connections.*", 2));
      logsFrom(postgres, "postgres");
      postgres.start();

      var spicedbUri = "postgres://postgres:postgres@postgres:5432/spicedb?sslmode=disable";
      var openfgaUri = "postgres://postgres:postgres@postgres:5432/openfga?sslmode=disable";
      // Each engine requires a one-shot schema migration before it will serve on Postgres.
      migrate(
          "authzed/spicedb:latest",
          "datastore",
          "migrate",
          "head",
          "--datastore-engine",
          "postgres",
          "--datastore-conn-uri",
          spicedbUri);
      migrate(
          "openfga/openfga:latest",
          "migrate",
          "--datastore-engine",
          "postgres",
          "--datastore-uri",
          openfgaUri);

      spicedb =
          spicedbContainer()
              .withCommand(
                  "serve",
                  "--grpc-preshared-key",
                  SPICEDB_KEY,
                  "--datastore-engine",
                  "postgres",
                  "--datastore-conn-uri",
                  spicedbUri);
      openfga =
          openfgaContainer()
              .withCommand(
                  "run",
                  "--http-addr",
                  "0.0.0.0:9190",
                  "--datastore-engine",
                  "postgres",
                  "--datastore-uri",
                  openfgaUri);
    } else {
      spicedb = spicedbContainer().withCommand("serve", "--grpc-preshared-key", SPICEDB_KEY);
      openfga = openfgaContainer().withCommand("run", "--http-addr", "0.0.0.0:9190");
    }
    spicedb.start();
    openfga.start();
    log.info("Benchmark datastore: {}", POSTGRES ? "postgres" : "memory");
  }

  @AfterAll
  static void stopStores() {
    if (openfga != null) {
      openfga.stop();
    }
    if (spicedb != null) {
      spicedb.stop();
    }
    if (postgres != null) {
      postgres.stop();
    }
    NETWORK.close();
  }

  private static GenericContainer<?> spicedbContainer() {
    var container =
        new GenericContainer<>("authzed/spicedb:latest")
            .withExposedPorts(50051)
            .waitingFor(Wait.forListeningPort());
    if (POSTGRES) {
      container.withNetwork(NETWORK);
    }
    logsFrom(container, "spicedb");
    return container;
  }

  private static GenericContainer<?> openfgaContainer() {
    var container =
        new GenericContainer<>("openfga/openfga:latest")
            .withExposedPorts(9190)
            .waitingFor(Wait.forHttp("/healthz").forPort(9190).forStatusCode(200));
    if (POSTGRES) {
      container.withNetwork(NETWORK);
    }
    logsFrom(container, "openfga");
    return container;
  }

  /** Forward a container's stdout/stderr to the {@code container.<name>} logger, when enabled. */
  private static void logsFrom(GenericContainer<?> container, String name) {
    if (CONTAINER_LOGS) {
      container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container." + name)));
    }
  }

  /** Run a one-shot datastore migration to completion (exit 0) before the server starts. */
  private static void migrate(String image, String... command) {
    try (GenericContainer<?> migrate =
        new GenericContainer<>(image)
            .withNetwork(NETWORK)
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2)))
            .withCommand(command)) {
      migrate.start();
    }
  }

  static Stream<ZanzibarStore> stores() {
    return Stream.of(
        new SpiceDbZanzibarStore(
            TestTypes.SCHEMA, spicedb.getHost(), spicedb.getMappedPort(50051), SPICEDB_KEY),
        new OpenFgaZanzibarStore(
            TestTypes.SCHEMA, "http://" + openfga.getHost() + ":" + openfga.getMappedPort(9190)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("stores")
  void benchmark(ZanzibarStore engineStore) {
    var store = new RateLimitedZanzibarStore(engineStore, RateLimiter.create(WRITE_RATE));
    var engine = store.providerId();
    store.applySchema();
    var pipeline = new IngestionPipeline(store, INGEST_FLUSH);

    // Expected source emissions, so progress can show a percentage: one membership per user role,
    // plus one grant per (perType x type) resource.
    long roleTotal = (long) USERS * ROLES_PER_USER;
    long resourceTotal = (long) PER_TYPE * TestTypes.SCHEMA.getResourceTypes().size();

    var start = System.nanoTime();
    long roleRels =
        pipeline.ingestRoles(
            withProgress(
                engine,
                "role memberships",
                roleTotal,
                SyntheticData.roleMemberships(GROUPS, USERS, ROLES_PER_USER, 1)));
    long roleMs = millisSince(start);

    start = System.nanoTime();
    long resourceRels =
        pipeline.ingestResources(
            withProgress(
                engine,
                "resources",
                resourceTotal,
                SyntheticData.resources(PER_TYPE, 0.05, GROUPS, ROLES_PER_RESOURCE, 2)));
    long resourceMs = millisSince(start);

    log.info(
        "[{}] scale: {} users x ~{} roles, {} groups, {} resources/type x {} types",
        engine,
        USERS,
        ROLES_PER_USER,
        GROUPS,
        PER_TYPE,
        TestTypes.SCHEMA.getResourceTypes().size());
    log.info(
        "[{}] ingest: {} role rels in {} ms ({}/s); {} resource rels in {} ms ({}/s)",
        engine,
        roleRels,
        roleMs,
        perSec(roleRels, roleMs),
        resourceRels,
        resourceMs,
        perSec(resourceRels, resourceMs));

    // A guaranteed-allow hot resource for the heaviest user (u0 already holds ROLES_PER_USER
    // roles).
    store.addMember("g0", "user:u0");
    for (var type : TestTypes.SCHEMA.getResourceTypes()) {
      store.addAcl(type, type + "-hot", READ, "group:g0");
    }

    var rnd = new Random(7);

    // check (mixed): random heavy user vs. random application — sparse, so mostly deny (the
    // full-expansion worst case). The allow-rate is reported so the mix is visible.
    var mixed = new long[CHECK_SAMPLES];
    int allowed = 0;
    for (int i = 0; i < warmup(); i++) {
      store.check(
          "u" + rnd.nextInt(USERS), APPLICATION, "application-" + rnd.nextInt(PER_TYPE), READ);
    }
    for (int i = 0; i < CHECK_SAMPLES; i++) {
      var user = "u" + rnd.nextInt(USERS);
      var name = "application-" + rnd.nextInt(PER_TYPE);
      var t = System.nanoTime();
      var ok = store.check(user, APPLICATION, name, READ);
      mixed[i] = System.nanoTime() - t;
      if (ok) {
        allowed++;
      }
    }
    report(engine, "check(mixed)", mixed, allowed + "/" + CHECK_SAMPLES + " allowed");

    // check (allow): the heaviest user on a resource they can reach — positive full expansion.
    var allow = new long[CHECK_SAMPLES];
    for (int i = 0; i < warmup(); i++) {
      store.check("u0", APPLICATION, "application-hot", READ);
    }
    for (int i = 0; i < CHECK_SAMPLES; i++) {
      var t = System.nanoTime();
      store.check("u0", APPLICATION, "application-hot", READ);
      allow[i] = System.nanoTime() - t;
    }
    report(engine, "check(allow, heaviest user)", allow, "");

    // lookupResources: the View-enumeration Fiat runs per type on every getUserPermission.
    var lookup = new long[LOOKUP_SAMPLES];
    long totalFound = 0;
    for (int i = 0; i < LOOKUP_SAMPLES; i++) {
      var user = "u" + rnd.nextInt(USERS);
      var t = System.nanoTime();
      var found = store.lookupResources(user, APPLICATION, READ);
      lookup[i] = System.nanoTime() - t;
      totalFound += found.size();
    }
    report(
        engine,
        "lookupResources(application)",
        lookup,
        "avg " + (totalFound / LOOKUP_SAMPLES) + " results");

    // isMember: admin resolution — a transitive membership check against the root group.
    var member = new long[CHECK_SAMPLES];
    for (int i = 0; i < warmup(); i++) {
      store.isMember("u" + rnd.nextInt(USERS), "g0");
    }
    for (int i = 0; i < CHECK_SAMPLES; i++) {
      var user = "u" + rnd.nextInt(USERS);
      var t = System.nanoTime();
      store.isMember(user, "g0");
      member[i] = System.nanoTime() - t;
    }
    report(engine, "isMember(root group)", member, "");

    store.close();
  }

  private static int warmup() {
    return Math.min(100, CHECK_SAMPLES / 10);
  }

  /**
   * Decorate a source so ingest logs a cumulative count at most every {@link
   * #PROGRESS_INTERVAL_MS}.
   */
  private static RoleSource withProgress(
      String engine, String unit, long total, RoleSource source) {
    var tracker = new ProgressTracker(engine, unit, total);
    return consumer ->
        source.forEach(
            (groupId, memberRef) -> {
              consumer.accept(groupId, memberRef);
              tracker.tick();
            });
  }

  private static ResourceSource withProgress(
      String engine, String unit, long total, ResourceSource source) {
    var tracker = new ProgressTracker(engine, unit, total);
    return consumer ->
        source.forEach(
            (type, name, permissions) -> {
              consumer.accept(type, name, permissions);
              tracker.tick();
            });
  }

  private static final class ProgressTracker {
    private final String engine;
    private final String unit;
    private final long total;
    private long count;
    private long lastLog = System.currentTimeMillis();

    ProgressTracker(String engine, String unit, long total) {
      this.engine = engine;
      this.unit = unit;
      this.total = total;
    }

    void tick() {
      count++;
      var now = System.currentTimeMillis();
      if (now - lastLog >= PROGRESS_INTERVAL_MS) {
        var pct = total > 0 ? 100 * count / total : 0;
        log.info("[{}] ingested {}/{} {} ({}%)...", engine, count, total, unit, pct);
        lastLog = now;
      }
    }
  }

  private static void report(String engine, String name, long[] nanos, String note) {
    Arrays.sort(nanos);
    log.info(
        "[{}] {}: p50={} p95={} p99={} max={} ms (n={}) {}",
        engine,
        name,
        ms(percentile(nanos, 50)),
        ms(percentile(nanos, 95)),
        ms(percentile(nanos, 99)),
        ms(nanos[nanos.length - 1]),
        nanos.length,
        note);
  }

  private static long percentile(long[] sorted, int p) {
    var idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
  }

  private static String ms(long nanos) {
    return String.format("%.2f", nanos / 1_000_000.0);
  }

  private static long millisSince(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }

  private static long perSec(long count, long millis) {
    return millis == 0 ? count : count * 1000 / millis;
  }
}
