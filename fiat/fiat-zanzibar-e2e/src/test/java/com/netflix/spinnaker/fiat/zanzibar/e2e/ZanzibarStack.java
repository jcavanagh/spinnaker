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

package com.netflix.spinnaker.fiat.zanzibar.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.spicedb.SpiceDbZanzibarStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mysql.MySQLContainer;

/**
 * SpiceDB, MySQL and Redis in containers, and Fiat (two replicas), Front50, Clouddriver, Gate,
 * Orca, Igor and Echo as local processes. Every service reads {@code src/test/resources/config},
 * with this run's ports and hosts passed as {@code e2e.*} system properties.
 */
public class ZanzibarStack implements AutoCloseable {

  public static final String FIAT = "fiat";
  public static final String FIAT_REPLICA = "fiat-replica";
  public static final String FRONT50 = "front50";
  public static final String CLOUDDRIVER = "clouddriver";
  public static final String GATE = "gate";
  public static final String ORCA = "orca";
  public static final String IGOR = "igor";
  public static final String ECHO = "echo";

  // Roles and static resources named in src/test/resources/config.
  public static final String ENG = "e2e-eng";
  public static final String OPS = "e2e-ops";
  public static final String ADMINS = "e2e-admins";
  public static final String ACCOUNT_MANAGERS = "e2e-account-managers";
  public static final String ACCOUNT_OPEN = "e2e-k8s-open";
  public static final String ACCOUNT_ENG = "e2e-k8s-eng";
  public static final String ACCOUNT_OPS = "e2e-k8s-ops";
  public static final String JENKINS_OPEN = "e2e-jenkins-open";
  public static final String JENKINS_ENG = "e2e-jenkins-eng";

  /** A Fiat admin, in every run. */
  public static final String ROOT = "root@e2e.io";

  private static final Logger log = LogManager.getLogger(ZanzibarStack.class);
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private static final String SPICEDB_KEY = "zanzibar-e2e-key";
  private static final List<String> CONFIG_FILES =
      List.of(
          "spinnaker.yml",
          "fiat.yml",
          "front50.yml",
          "clouddriver.yml",
          "gate.yml",
          "orca.yml",
          "igor.yml",
          "echo.yml",
          "kubeconfig.yml");

  private final Path repoRoot = Path.of(System.getProperty("zanzibar.e2e.repoRoot"));
  private final Path workDir = Path.of(System.getProperty("zanzibar.e2e.workDir"));
  private final Path rolesFile = workDir.resolve("roles.yml");
  private final Map<String, List<String>> roles = new TreeMap<>(Map.of(ROOT, List.of(ADMINS)));
  private final Map<String, ServiceProcess> services = new LinkedHashMap<>();
  private ZanzibarStore store;

  private final GenericContainer<?> spicedb =
      new GenericContainer<>("authzed/spicedb:latest")
          .withCommand("serve", "--grpc-preshared-key", SPICEDB_KEY)
          .withExposedPorts(50051)
          .waitingFor(Wait.forListeningPort());

  private final GenericContainer<?> redis =
      new GenericContainer<>("valkey/valkey:8").withExposedPorts(6379);

  private final MySQLContainer mysql =
      new MySQLContainer("mysql:8.0.37")
          .withDatabaseName("front50")
          .withUsername("root")
          .withPassword("root")
          .withInitScript("mysql-init.sql");

  public static ZanzibarStack start() throws Exception {
    var stack = new ZanzibarStack();
    try {
      stack.startAll();
      return stack;
    } catch (Exception e) {
      stack.close();
      throw e;
    }
  }

  private void startAll() throws Exception {
    Files.createDirectories(workDir.resolve("logs"));
    Files.createDirectories(workDir.resolve("home"));
    var configDir = Files.createDirectories(workDir.resolve("config"));
    for (var file : CONFIG_FILES) {
      try (var in =
          Objects.requireNonNull(getClass().getResourceAsStream("/config/" + file), file)) {
        Files.copy(in, configDir.resolve(file), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    writeRoles();

    Stream.of(spicedb, redis, mysql).parallel().forEach(GenericContainer::start);

    var ports = new LinkedHashMap<String, Integer>();
    for (var name : List.of(FIAT, FIAT_REPLICA, FRONT50, CLOUDDRIVER, GATE, ORCA, IGOR, ECHO)) {
      ports.put(name, freePort());
    }
    var options = new ArrayList<String>();
    options.add("-Xmx1g");
    options.add("-XX:TieredStopAtLevel=1");
    // Keeps services off the developer's ~/.spinnaker configuration.
    options.add("-Duser.home=" + workDir.resolve("home"));
    options.add("-Dspring.config.import=optional:file:" + configDir + "/");
    options.add("-De2e.config.dir=" + configDir);
    options.add("-De2e.roles.file=" + rolesFile);
    options.add("-De2e.spicedb.host=" + spicedb.getHost());
    options.add("-De2e.spicedb.port=" + spicedb.getMappedPort(50051));
    options.add("-De2e.spicedb.key=" + SPICEDB_KEY);
    options.add("-De2e.redis.url=redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    options.add("-De2e.mysql.host=" + mysql.getHost());
    options.add("-De2e.mysql.port=" + mysql.getMappedPort(3306));
    ports.forEach((name, port) -> options.add("-De2e.port." + name + "=" + port));

    ports.forEach(
        (name, port) -> {
          var distribution = name.equals(FIAT_REPLICA) ? FIAT : name;
          services.put(
              name,
              new ServiceProcess(
                  name,
                  repoRoot
                      .resolve(distribution)
                      .resolve(distribution + "-web/build/install")
                      .resolve(distribution),
                  port,
                  workDir.resolve("logs"),
                  options));
        });
    // Igor and Fiat check their dependencies once at startup, and Gate checks all of them.
    startAndAwait(FRONT50, CLOUDDRIVER, ORCA, ECHO);
    startAndAwait(IGOR, FIAT, FIAT_REPLICA);
    startAndAwait(GATE);
    // Static accounts and build services reach the store only through the bootstrap.
    log.info("Bootstrap: {}", new SpinnakerApi(this).bootstrap(FIAT));
  }

  private void startAndAwait(String... names) throws IOException {
    for (var name : names) {
      service(name).start();
      log.info("Started {}", service(name));
    }
    Stream.of(names)
        .map(
            name ->
                CompletableFuture.runAsync(
                    () -> {
                      try {
                        service(name).awaitHealthy(Duration.ofMinutes(5));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                      }
                    }))
        .toList()
        .forEach(CompletableFuture::join);
  }

  /** Restarts a service with the same configuration and port. */
  public void restart(String name) throws Exception {
    var service = service(name);
    service.stop();
    service.start();
    service.awaitHealthy(Duration.ofMinutes(5));
  }

  /** The SpiceDB store Fiat uses, read and written directly. */
  public synchronized ZanzibarStore store() {
    if (store == null) {
      store =
          new SpiceDbZanzibarStore(
              new ZanzibarSchema(List.of("account", "application", "build_service")),
              spicedb.getHost(),
              spicedb.getMappedPort(50051),
              SPICEDB_KEY);
    }
    return store;
  }

  public ServiceProcess service(String name) {
    return Objects.requireNonNull(services.get(name), name);
  }

  public String url(String service) {
    return service(service).baseUrl();
  }

  /** Replaces {@code user}'s roles in Fiat's file role provider; read at the user's next login. */
  public synchronized void setRoles(String user, String... userRoles) {
    roles.put(user, List.of(userRoles));
    writeRoles();
  }

  private synchronized void writeRoles() {
    var users = new ArrayList<Map<String, Object>>();
    roles.forEach(
        (user, names) ->
            users.add(
                Map.of(
                    "username",
                    user,
                    "roles",
                    names.stream().map(name -> Map.of("name", name)).toList())));
    try {
      var tmp = rolesFile.resolveSibling("roles.yml.tmp");
      YAML.writeValue(tmp.toFile(), Map.of("users", users));
      Files.move(
          tmp, rolesFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    if (store != null) {
      store.close();
    }
    services.values().parallelStream().forEach(ServiceProcess::stop);
    Stream.of(spicedb, redis, mysql).parallel().forEach(GenericContainer::stop);
  }

  private static int freePort() throws IOException {
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
