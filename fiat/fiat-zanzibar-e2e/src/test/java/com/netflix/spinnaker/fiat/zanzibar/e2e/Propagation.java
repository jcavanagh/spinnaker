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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Measures how long each service takes to reach a decision after a change, and appends the result
 * to {@code build/zanzibar-e2e/propagation.md}.
 */
public final class Propagation {

  /** fiat-api's View cache in every service ({@code services.fiat.cache}), by default. */
  public static final Duration VIEW_CACHE_TTL = Duration.ofSeconds(20);

  /** Fiat's store cache ({@code fiat.zanzibar.cache-ttl}), by default. */
  public static final Duration STORE_CACHE_TTL = Duration.ofSeconds(10);

  /** Event delivery, polling and request time. */
  public static final Duration SLACK = Duration.ofSeconds(5);

  private static final Logger log = LogManager.getLogger(Propagation.class);
  private static final Duration POLL = Duration.ofMillis(200);
  private static final Path REPORT =
      Path.of(System.getProperty("zanzibar.e2e.workDir"), "propagation.md");

  private Propagation() {}

  /**
   * Checks that every probe returns {@code decision} now. Run before a change, it also loads each
   * service's View cache, so the measurement after the change includes a stale cached View.
   */
  public static void assertNow(
      String scenario, Map<String, Callable<Boolean>> probes, boolean decision) throws Exception {
    for (var probe : probes.entrySet()) {
      assertThat(probe.getValue().call())
          .as("%s: %s before the change", scenario, probe.getKey())
          .isEqualTo(decision);
    }
  }

  /**
   * Polls every probe until it returns {@code decision}, and returns how long each took after
   * {@code changedAt}. Fails if any probe has not changed within {@code timeout}.
   */
  public static Map<String, Duration> await(
      String scenario,
      Instant changedAt,
      Map<String, Callable<Boolean>> probes,
      boolean decision,
      Duration timeout) {
    var latencies = new LinkedHashMap<String, Duration>();
    var errors = new LinkedHashMap<String, String>();
    var deadline = changedAt.plus(timeout);
    while (latencies.size() < probes.size() && Instant.now().isBefore(deadline)) {
      for (var probe : probes.entrySet()) {
        if (latencies.containsKey(probe.getKey())) {
          continue;
        }
        try {
          if (probe.getValue().call() == decision) {
            latencies.put(probe.getKey(), Duration.between(changedAt, Instant.now()));
          }
        } catch (Exception e) {
          errors.put(probe.getKey(), e.getMessage());
        }
      }
      SpinnakerApi.sleep(POLL);
    }
    report(scenario, probes, latencies);
    var pending =
        probes.keySet().stream()
            .filter(name -> !latencies.containsKey(name))
            .map(name -> name + (errors.containsKey(name) ? " (" + errors.get(name) + ")" : ""))
            .collect(Collectors.toList());
    assertThat(pending).as("%s: services still stale after %s", scenario, timeout).isEmpty();
    return latencies;
  }

  /** Asserts the named services changed within {@code bound}. */
  public static void assertWithin(
      Map<String, Duration> latencies, Duration bound, String... services) {
    for (var service : services) {
      assertThat(latencies.get(service)).as("%s latency", service).isLessThanOrEqualTo(bound);
    }
  }

  private static synchronized void report(
      String scenario, Map<String, Callable<Boolean>> probes, Map<String, Duration> latencies) {
    var cells =
        probes.keySet().stream()
            .map(
                name ->
                    name
                        + " "
                        + (latencies.containsKey(name)
                            ? latencies.get(name).toMillis() + " ms"
                            : "timed out"))
            .collect(Collectors.joining(", "));
    log.info("{}: {}", scenario, cells);
    try {
      Files.writeString(
          REPORT,
          "- " + scenario + ": " + cells + "\n",
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
