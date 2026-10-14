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

import static com.netflix.spinnaker.fiat.zanzibar.e2e.Propagation.SLACK;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.Propagation.STORE_CACHE_TTL;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.createApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.updateApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT_REPLICA;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.OPS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Every write lands on one Fiat replica, which evicts its own store cache. Another replica reads
 * the store directly, behind its own cache.
 */
@ExtendWith(ZanzibarStackExtension.class)
@Order(4)
class ReplicaConsistencyTest {

  static final String OWNER = "replica-owner@e2e.io";
  static final String READER = "replica-reader@e2e.io";
  static final String APP = "e2ereplica";

  static ZanzibarStack stack;
  static SpinnakerApi api;

  @BeforeAll
  static void createData(ZanzibarStack zanzibarStack) {
    stack = zanzibarStack;
    api = new SpinnakerApi(stack);
    stack.setRoles(OWNER, ENG);
    stack.setRoles(READER, OPS);
    List.of(OWNER, READER).forEach(api::login);
    var task =
        api.runTask(
            OWNER,
            APP,
            createApplication(APP, Map.of("READ", List.of(ENG), "WRITE", List.of(ENG))));
    assertThat(task.path("status").asText()).isEqualTo("SUCCEEDED");
  }

  @Test
  void resourceChangeReachesTheOtherReplica() throws Exception {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    for (var fiat : List.of(FIAT, FIAT_REPLICA)) {
      probes.put(
          fiat, () -> api.authorizations(fiat, READER, "applications", APP).contains("READ"));
    }
    Propagation.assertNow("before the grant", probes, false);

    var task =
        api.runTask(
            OWNER,
            APP,
            updateApplication(APP, Map.of("READ", List.of(ENG, OPS), "WRITE", List.of(ENG))));
    assertThat(task.path("status").asText()).isEqualTo("SUCCEEDED");
    var granted = Instant.now();
    var latencies =
        Propagation.await(
            "grant on one replica, read on both",
            granted,
            probes,
            true,
            STORE_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT);
  }

  /** Memberships are read uncached, and resource lookups through the store cache. */
  @Test
  void loginReachesTheOtherReplica() throws Exception {
    var user = "replica-joiner@e2e.io";
    stack.setRoles(user);
    api.login(user);
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    for (var fiat : List.of(FIAT, FIAT_REPLICA)) {
      probes.put("roles/" + fiat, () -> api.roles(fiat, user).contains(ENG));
      probes.put(
          "access/" + fiat,
          () -> api.authorizations(fiat, user, "applications", APP).contains("READ"));
    }
    Propagation.assertNow("before joining eng", probes, false);

    stack.setRoles(user, ENG);
    var loggedIn = Instant.now();
    api.login(user);
    var latencies =
        Propagation.await(
            "login on one replica, read on both",
            loggedIn,
            probes,
            true,
            STORE_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(
        latencies, SLACK, "roles/" + FIAT, "access/" + FIAT, "roles/" + FIAT_REPLICA);
  }
}
