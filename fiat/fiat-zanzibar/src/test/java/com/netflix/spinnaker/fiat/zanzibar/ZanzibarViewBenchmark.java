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

import static com.netflix.spinnaker.security.Authorization.READ;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.config.AccountManagerConfig;
import com.netflix.spinnaker.fiat.config.FiatAdminConfig;
import com.netflix.spinnaker.fiat.model.UserPermission;
import com.netflix.spinnaker.fiat.model.resources.Account;
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.BuildService;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Role;
import com.netflix.spinnaker.fiat.permissions.DefaultFallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.testing.ProductionShapedFixtures;
import com.netflix.spinnaker.fiat.zanzibar.resource.AccountResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ApplicationResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.BuildServiceResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.CachingZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarRelationship;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.IngestionPipeline;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.openfga.OpenFgaZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import com.netflix.spinnaker.security.Authorization;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * Benchmarks the operation every {@code fiat-api} client actually calls: the full {@code
 * UserPermission.View} built by {@link ZanzibarPermissionsRepository#get}. Clients never call
 * Fiat's point-check endpoint; they fetch the View on each cache miss and evaluate it locally.
 *
 * <p>Per engine it reports:
 *
 * <ul>
 *   <li><b>real-ID probe</b> — whether the store accepts production identifier shapes (emails,
 *       URN-style user ids, an email used as a role, dotted resource names).
 *   <li><b>View build</b> — latency of one full View against the raw store, the store calls it
 *       makes, and the resources returned versus the resources the user can actually access.
 *   <li><b>per-request re-login</b> — the {@code PUT /roles} path some auth filters run on every
 *       request: an unchanged membership write, then the first View after it, through the
 *       production read cache.
 * </ul>
 *
 * <p>The dataset is {@link ProductionShapedFixtures}, the same one {@code FiatBaselineBenchmark}
 * uses. Its identifiers are restricted to characters every engine accepts; the probe covers real
 * identifier shapes separately.
 *
 * <p>Docker-gated: runs only with {@code -Dfiat.zanzibar.benchmark=true}. Pick engines with {@code
 * -Dfiat.zanzibar.bench.engines=spicedb,openfga} and dial scale with {@code fiat.zanzibar.bench.*}.
 */
@EnabledIfSystemProperty(named = "fiat.zanzibar.benchmark", matches = "true")
class ZanzibarViewBenchmark {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<ZanzibarResourceType> TYPES =
      List.of(
          new AccountResourceType(Account::getPermissions, null, MAPPER),
          new ApplicationResourceType(
              Application::getPermissions,
              null,
              MAPPER,
              new DefaultFallbackPermissionsResolver(
                  com.netflix.spinnaker.fiat.model.Authorization.EXECUTE,
                  com.netflix.spinnaker.fiat.model.Authorization.READ),
              false),
          new BuildServiceResourceType(BuildService::getPermissions, null, MAPPER));

  private static final String PREFIX = "fiat.zanzibar.bench.";
  private static final List<String> ENGINES =
      Arrays.asList(System.getProperty(PREFIX + "engines", "spicedb,openfga").split(","));
  private static final int USERS = Integer.getInteger(PREFIX + "users", 200);
  private static final int ROLES_PER_USER = Integer.getInteger(PREFIX + "rolesPerUser", 1000);
  private static final int GROUPS = Integer.getInteger(PREFIX + "groups", 20000);
  private static final int ACCOUNTS = Integer.getInteger(PREFIX + "accounts", 10000);
  private static final int APPLICATIONS = Integer.getInteger(PREFIX + "applications", 7000);
  private static final int ROLES_PER_RESOURCE = Integer.getInteger(PREFIX + "rolesPerResource", 5);
  private static final double UNRESTRICTED_FRACTION =
      Double.parseDouble(System.getProperty(PREFIX + "unrestrictedFraction", "0.05"));
  private static final int VIEW_SAMPLES = Integer.getInteger(PREFIX + "viewSamples", 10);
  private static final int LOGIN_SAMPLES = Integer.getInteger(PREFIX + "loginSamples", 5);
  private static final int FLUSH_SIZE = Integer.getInteger(PREFIX + "flushSize", 5000);
  private static final int LOOKUP_PARALLELISM =
      Integer.getInteger(PREFIX + "lookupParallelism", 32);

  /** OpenFGA's ListObjects deadline (e.g. {@code 60s}); unset keeps the server default. */
  private static final String OPENFGA_LIST_DEADLINE =
      System.getProperty(PREFIX + "openfgaListDeadline");

  private static final long PROGRESS_INTERVAL_MS = 5000;

  private static final String SPICEDB_KEY = "fiat-view-bench-key";
  private static final Network NETWORK = Network.newNetwork();

  private static GenericContainer<?> postgres;
  private static GenericContainer<?> spicedb;
  private static GenericContainer<?> openfga;

  @BeforeAll
  static void startStores() {
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
    postgres.start();

    if (ENGINES.contains("spicedb")) {
      var uri = "postgres://postgres:postgres@postgres:5432/spicedb?sslmode=disable";
      migrate(
          "authzed/spicedb:latest",
          "datastore",
          "migrate",
          "head",
          "--datastore-engine",
          "postgres",
          "--datastore-conn-uri",
          uri);
      spicedb =
          new GenericContainer<>("authzed/spicedb:latest")
              .withNetwork(NETWORK)
              .withCommand(
                  "serve",
                  "--grpc-preshared-key",
                  SPICEDB_KEY,
                  "--datastore-engine",
                  "postgres",
                  "--datastore-conn-uri",
                  uri)
              .withExposedPorts(50051)
              .waitingFor(Wait.forListeningPort());
      spicedb.start();
    }
    if (ENGINES.contains("openfga")) {
      var uri = "postgres://postgres:postgres@postgres:5432/openfga?sslmode=disable";
      migrate(
          "openfga/openfga:latest",
          "migrate",
          "--datastore-engine",
          "postgres",
          "--datastore-uri",
          uri);
      openfga =
          new GenericContainer<>("openfga/openfga:latest")
              .withNetwork(NETWORK)
              .withCommand(
                  "run",
                  "--http-addr",
                  "0.0.0.0:9190",
                  "--datastore-engine",
                  "postgres",
                  "--datastore-uri",
                  uri)
              .withExposedPorts(9190)
              .waitingFor(Wait.forHttp("/healthz").forPort(9190).forStatusCode(200));
      if (OPENFGA_LIST_DEADLINE != null) {
        // OpenFGA refuses to start with a list deadline above its request timeouts.
        openfga
            .withEnv("OPENFGA_LIST_OBJECTS_DEADLINE", OPENFGA_LIST_DEADLINE)
            .withEnv("OPENFGA_REQUEST_TIMEOUT", OPENFGA_LIST_DEADLINE)
            .withEnv("OPENFGA_HTTP_UPSTREAM_TIMEOUT", OPENFGA_LIST_DEADLINE);
      }
      openfga.start();
    }
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

  @Test
  void benchmark() {
    log(
        "scale: %d users x %d roles from %d groups; %d accounts + %d applications (%d READ + 1"
            + " WRITE groups each, %d%% unrestricted); lookup parallelism %d; openfga list"
            + " deadline %s",
        USERS,
        ROLES_PER_USER,
        GROUPS,
        ACCOUNTS,
        APPLICATIONS,
        ROLES_PER_RESOURCE,
        Math.round(UNRESTRICTED_FRACTION * 100),
        LOOKUP_PARALLELISM,
        OPENFGA_LIST_DEADLINE != null ? OPENFGA_LIST_DEADLINE : "default");
    for (var engine : ENGINES) {
      try (var store = open(engine)) {
        run(engine, store);
      }
    }
  }

  private void run(String engine, ZanzibarStore store) {
    store.applySchema();
    probeRealIds(engine, store);
    ingest(engine, store);

    var properties = new FiatZanzibarProperties();
    properties.setLookupParallelism(LOOKUP_PARALLELISM);
    var adminConfig = new FiatAdminConfig();
    adminConfig.getAdmin().setRoles(List.of(ProductionShapedFixtures.adminGroupId(GROUPS)));
    var counting = new CountingStore(store);
    var repository =
        new ZanzibarPermissionsRepository(
            counting,
            TYPES,
            new Reconciler(counting),
            new InProcessReconcileLock(),
            properties,
            adminConfig,
            new AccountManagerConfig(),
            null);
    var rnd = new Random(11);

    // View build against the raw store: what a fiat-api cache miss costs. Fiat's controller also
    // computes the View from the permission, so both steps are timed.
    var view = new long[VIEW_SAMPLES];
    var fullView = new long[VIEW_SAMPLES];
    long calls = 0;
    long returned = 0;
    long expected = 0;
    for (int s = 0; s < VIEW_SAMPLES; s++) {
      var user = rnd.nextInt(USERS);
      counting.reset();
      var t = System.nanoTime();
      var permission = repository.get(ProductionShapedFixtures.userId(user)).orElseThrow();
      view[s] = System.nanoTime() - t;
      permission.getView();
      fullView[s] = System.nanoTime() - t;
      calls += counting.calls();
      returned += permission.getAccounts().size() + permission.getApplications().size();
      expected += accessible(user);
    }
    report(
        engine,
        "View build (raw store)",
        view,
        String.format(
            "avg %d store calls; avg %d accounts+apps returned of %d accessible",
            calls / VIEW_SAMPLES, returned / VIEW_SAMPLES, expected / VIEW_SAMPLES));
    report(engine, "View build + getView (what /authorize returns)", fullView, "");
    if (returned < expected) {
      log(
          "[%s] View is TRUNCATED: returned %d of %d accessible accounts+apps across samples",
          engine, returned, expected);
    }

    // Per-request re-login: an unchanged membership write, then the View read after it, via the
    // prod cache.
    var cached = new CachingZanzibarStore(new CountingStore(store), Duration.ofSeconds(10), 50_000);
    var cachedRepository =
        new ZanzibarPermissionsRepository(
            cached,
            TYPES,
            new Reconciler(cached),
            new InProcessReconcileLock(),
            properties,
            adminConfig,
            new AccountManagerConfig(),
            null);
    var write = new long[LOGIN_SAMPLES];
    var firstView = new long[LOGIN_SAMPLES];
    var warmView = new long[LOGIN_SAMPLES];
    for (int s = 0; s < LOGIN_SAMPLES; s++) {
      var user = rnd.nextInt(USERS);
      var userId = ProductionShapedFixtures.userId(user);
      var roles = new LinkedHashSet<Role>();
      for (var role : ProductionShapedFixtures.rolesOf(user, ROLES_PER_USER, GROUPS)) {
        roles.add(new Role(role));
      }
      var login = new UserPermission().setId(userId);
      login.setRoles(roles);
      var t = System.nanoTime();
      cachedRepository.put(login);
      write[s] = System.nanoTime() - t;
      t = System.nanoTime();
      cachedRepository.get(userId);
      firstView[s] = System.nanoTime() - t;
      t = System.nanoTime();
      cachedRepository.get(userId);
      warmView[s] = System.nanoTime() - t;
    }
    report(engine, "re-login: membership write (no change)", write, "");
    report(engine, "re-login: first View after the write", firstView, "");
    report(engine, "View, warm cache (no writes in between)", warmView, "");
  }

  /** Try production identifier shapes one at a time; report what the engine accepts. */
  private void probeRealIds(String engine, ZanzibarStore store) {
    probe(
        engine,
        store,
        "email user id",
        ZanzibarRelationship.member("1000001", "user:jane.doe@example.com"));
    probe(
        engine,
        store,
        "URN-style user id",
        ZanzibarRelationship.member("1000001", "user:urn:example:person:1234567"));
    probe(
        engine,
        store,
        "email used as a role (X509)",
        ZanzibarRelationship.member("jane.doe@example.com", "user:user-1"));
    probe(
        engine,
        store,
        "dotted account name",
        ZanzibarRelationship.acl("account", "prod.us-west-2", READ, "group:1000001"));
    probe(
        engine,
        store,
        "control: numeric group, hyphenated app",
        ZanzibarRelationship.acl("application", "my-app", READ, "group:1000001"));
  }

  private static void probe(
      String engine, ZanzibarStore store, String label, ZanzibarRelationship tuple) {
    try {
      store.write(List.of(tuple));
      store.delete(List.of(tuple));
      log("[%s] real-ID probe: %s -> accepted", engine, label);
    } catch (RuntimeException e) {
      var message = rootMessage(e);
      log(
          "[%s] real-ID probe: %s -> REJECTED (%s)",
          engine, label, message.length() > 160 ? message.substring(0, 160) + "..." : message);
    }
  }

  private void ingest(String engine, ZanzibarStore store) {
    var pipeline = new IngestionPipeline(store, FLUSH_SIZE);
    var start = System.nanoTime();
    long resources =
        pipeline.ingestResources(
            consumer -> {
              for (int i = 0; i < ACCOUNTS; i++) {
                var name = ProductionShapedFixtures.accountName(i);
                consumer.accept("account", name, ZanzibarTypes.grants(permissions(name)));
              }
              for (int i = 0; i < APPLICATIONS; i++) {
                var name = ProductionShapedFixtures.applicationName(i);
                consumer.accept("application", name, ZanzibarTypes.grants(permissions(name)));
              }
            });
    var lastLog = new long[] {System.currentTimeMillis()};
    long memberships =
        pipeline.ingestRoles(
            consumer -> {
              for (int u = 0; u < USERS; u++) {
                var member = "user:" + ProductionShapedFixtures.userId(u);
                for (var role : ProductionShapedFixtures.rolesOf(u, ROLES_PER_USER, GROUPS)) {
                  consumer.accept(role, member);
                }
                var now = System.currentTimeMillis();
                if (now - lastLog[0] >= PROGRESS_INTERVAL_MS) {
                  log("[%s] ingested memberships for %d/%d users...", engine, u + 1, USERS);
                  lastLog[0] = now;
                }
              }
            });
    log(
        "[%s] setup ingest: %d ACL + %d membership tuples in %d ms",
        engine, resources, memberships, (System.nanoTime() - start) / 1_000_000);
  }

  /** Accounts + applications the user can act on: unrestricted, or sharing any ACL role. */
  private static long accessible(int user) {
    var roles = ProductionShapedFixtures.rolesOf(user, ROLES_PER_USER, GROUPS);
    long count = 0;
    for (int i = 0; i < ACCOUNTS; i++) {
      count += canAccess(permissions(ProductionShapedFixtures.accountName(i)), roles) ? 1 : 0;
    }
    for (int i = 0; i < APPLICATIONS; i++) {
      count += canAccess(permissions(ProductionShapedFixtures.applicationName(i)), roles) ? 1 : 0;
    }
    return count;
  }

  private static boolean canAccess(Permissions permissions, Set<String> roles) {
    return !permissions.isRestricted()
        || !permissions.getAuthorizations(List.copyOf(roles)).isEmpty();
  }

  private static Permissions permissions(String name) {
    return ProductionShapedFixtures.permissionsOf(
        name, ROLES_PER_RESOURCE, GROUPS, UNRESTRICTED_FRACTION);
  }

  private static ZanzibarStore open(String engine) {
    return switch (engine) {
      case "spicedb" ->
          new SpiceDbZanzibarStore(
              ZanzibarTypes.schema(TYPES),
              spicedb.getHost(),
              spicedb.getMappedPort(50051),
              SPICEDB_KEY);
      case "openfga" ->
          new OpenFgaZanzibarStore(
              ZanzibarTypes.schema(TYPES),
              "http://" + openfga.getHost() + ":" + openfga.getMappedPort(9190),
              null,
              OPENFGA_LIST_DEADLINE != null
                  ? Duration.parse("PT" + OPENFGA_LIST_DEADLINE.toUpperCase())
                  : Duration.ofSeconds(3));
      default -> throw new IllegalArgumentException("unknown engine: " + engine);
    };
  }

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

  private static String rootMessage(Throwable e) {
    var cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return String.valueOf(cause.getMessage()).replace('\n', ' ');
  }

  private static void report(String engine, String name, long[] nanos, String note) {
    Arrays.sort(nanos);
    log(
        "[%s] %s: p50=%s p95=%s max=%s ms (n=%d) %s",
        engine,
        name,
        ms(percentile(nanos, 50)),
        ms(percentile(nanos, 95)),
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

  private static void log(String format, Object... args) {
    System.out.println("[view-bench] " + String.format(format, args));
  }

  /** Counts the read calls a View build makes against the store. */
  private static final class CountingStore implements ZanzibarStore {

    private final ZanzibarStore delegate;
    private final AtomicLong calls = new AtomicLong();

    CountingStore(ZanzibarStore delegate) {
      this.delegate = delegate;
    }

    void reset() {
      calls.set(0);
    }

    long calls() {
      return calls.get();
    }

    @Override
    public String providerId() {
      return delegate.providerId();
    }

    @Override
    public void applySchema() {
      delegate.applySchema();
    }

    @Override
    public void write(Collection<ZanzibarRelationship> tuples) {
      delegate.write(tuples);
    }

    @Override
    public void delete(Collection<ZanzibarRelationship> tuples) {
      delegate.delete(tuples);
    }

    @Override
    public Set<ZanzibarRelationship> read(String objectType, String objectId) {
      return delegate.read(objectType, objectId);
    }

    @Override
    public Set<String> objectIds(String objectType) {
      return delegate.objectIds(objectType);
    }

    @Override
    public void apply(
        Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
      delegate.apply(writes, deletes);
    }

    @Override
    public boolean check(String userId, String type, String name, Authorization action) {
      calls.incrementAndGet();
      return delegate.check(userId, type, name, action);
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public boolean isMember(String userId, String groupId) {
      calls.incrementAndGet();
      return delegate.isMember(userId, groupId);
    }

    @Override
    public Set<String> groupsOf(String userId) {
      calls.incrementAndGet();
      return delegate.groupsOf(userId);
    }

    @Override
    public Set<String> lookupResources(String userId, String type, Authorization action) {
      calls.incrementAndGet();
      return delegate.lookupResources(userId, type, action);
    }

    @Override
    public void close() {
      // The benchmark closes the underlying store.
    }
  }
}
