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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;

/** One Spinnaker service, run from its installed distribution. */
public class ServiceProcess {

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  @Getter private final String name;
  private final Path distribution;
  @Getter private final int port;
  private final Path logFile;
  private final List<String> jvmOptions;
  private Process process;

  /**
   * @param distribution the service's {@code installDist} output, e.g. {@code
   *     fiat-web/build/install/fiat}
   */
  public ServiceProcess(
      String name, Path distribution, int port, Path logDir, List<String> jvmOptions) {
    this.name = name;
    this.distribution = distribution;
    this.port = port;
    this.logFile = logDir.resolve(name + ".log");
    this.jvmOptions = List.copyOf(jvmOptions);
  }

  public String baseUrl() {
    return "http://localhost:" + port;
  }

  public synchronized void start() throws IOException {
    var script = distribution.resolve("bin").resolve(distribution.getFileName().toString());
    if (!Files.isExecutable(script)) {
      throw new IllegalStateException(
          "Missing " + script + "; build it first (see fiat/fiat-zanzibar-e2e/README.md)");
    }
    var options = new ArrayList<>(jvmOptions);
    options.add("-Dserver.port=" + port);
    var builder =
        new ProcessBuilder(script.toString())
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
    builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
    builder.environment().put("JAVA_OPTS", String.join(" ", options));
    process = builder.start();
  }

  /** Waits until {@code /health} reports UP. */
  public void awaitHealthy(Duration timeout) throws InterruptedException {
    var deadline = Instant.now().plus(timeout);
    var request =
        HttpRequest.newBuilder(URI.create(baseUrl() + "/health"))
            .timeout(Duration.ofSeconds(5))
            .build();
    String last = "no response";
    while (Instant.now().isBefore(deadline)) {
      if (!process.isAlive()) {
        throw new IllegalStateException(name + " exited\n" + logTail());
      }
      try {
        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200 && response.body().contains("\"UP\"")) {
          return;
        }
        last = response.statusCode() + " " + response.body();
      } catch (IOException e) {
        last = e.toString();
      }
      Thread.sleep(1000);
    }
    throw new IllegalStateException(
        name + " not healthy after " + timeout + ": " + last + "\n" + logTail());
  }

  public synchronized void stop() {
    if (process == null) {
      return;
    }
    process.descendants().forEach(ProcessHandle::destroy);
    process.destroy();
    try {
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      process.destroyForcibly();
      Thread.currentThread().interrupt();
    }
    process = null;
  }

  public String logTail() {
    try (var lines = Files.lines(logFile)) {
      var all = lines.collect(Collectors.toList());
      return String.join("\n", all.subList(Math.max(0, all.size() - 80), all.size()));
    } catch (IOException e) {
      return "(no log at " + logFile + ")";
    }
  }

  @Override
  public String toString() {
    return name + Map.of("url", baseUrl(), "log", logFile);
  }
}
