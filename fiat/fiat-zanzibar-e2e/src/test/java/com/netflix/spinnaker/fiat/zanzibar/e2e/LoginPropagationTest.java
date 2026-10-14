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
import static com.netflix.spinnaker.fiat.zanzibar.e2e.Propagation.VIEW_CACHE_TTL;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.createApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ADMINS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.GATE;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.JENKINS_ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ROOT;
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
 * Memberships change when a user logs in through Gate. Each test measures how long the change takes
 * to reach every service.
 */
@ExtendWith(ZanzibarStackExtension.class)
@Order(2)
class LoginPropagationTest {

  static final String APP = "e2elogin";

  static ZanzibarStack stack;
  static SpinnakerApi api;

  @BeforeAll
  static void createData(ZanzibarStack zanzibarStack) {
    stack = zanzibarStack;
    api = new SpinnakerApi(stack);
    var task =
        api.runTask(
            ROOT, APP, createApplication(APP, Map.of("READ", List.of(ENG), "WRITE", List.of(ENG))));
    assertThat(task.path("status").asText()).isEqualTo("SUCCEEDED");
  }

  /** A first login: no service has a View for the user yet. */
  @Test
  void firstLoginGrantsAccess() {
    var user = "first-login@e2e.io";
    stack.setRoles(user, ENG);

    var loggedIn = Instant.now();
    api.login(user);
    var latencies =
        Propagation.await(
            "first login grants eng access",
            loggedIn,
            engAccess(user),
            true,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, "app read/" + FIAT, "app read/" + GATE);
  }

  /**
   * A group added in the directory takes effect at the next login. Services that cached the user's
   * View before the login keep it until it expires.
   */
  @Test
  void groupAddedAtNextLogin() throws Exception {
    var user = "joins-eng@e2e.io";
    stack.setRoles(user);
    api.login(user);
    Propagation.assertNow("before joining eng", engAccess(user), false);

    stack.setRoles(user, ENG);
    assertThat(api.roles(FIAT, user)).as("roles before the next login").isEmpty();

    var loggedIn = Instant.now();
    api.login(user);
    var latencies =
        Propagation.await(
            "group added, next login", loggedIn, engAccess(user), true, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, "app read/" + FIAT, "app read/" + GATE);
  }

  /** A group removed in the directory is revoked at the next login. */
  @Test
  void groupRemovedAtNextLogin() throws Exception {
    var user = "leaves-eng@e2e.io";
    stack.setRoles(user, ENG);
    api.login(user);
    Propagation.await(
        "before leaving eng", Instant.now(), engAccess(user), true, VIEW_CACHE_TTL.plus(SLACK));

    stack.setRoles(user);
    assertThat(api.roles(FIAT, user)).as("roles before the next login").containsExactly(ENG);

    var loggedIn = Instant.now();
    api.login(user);
    var latencies =
        Propagation.await(
            "group removed, next login",
            loggedIn,
            engAccess(user),
            false,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, "app read/" + FIAT, "app read/" + GATE);
  }

  /** Admin comes from the user's roles at read time, so it follows the login too. */
  @Test
  void adminGrantedAndRevokedAtNextLogin() throws Exception {
    var user = "temp-admin@e2e.io";
    stack.setRoles(user);
    api.login(user);
    var probes = new LinkedHashMap<>(api.applicationRead(user, APP));
    probes.put(FIAT, () -> api.view(FIAT, user).path("admin").asBoolean());
    Propagation.assertNow("before admin", probes, false);

    stack.setRoles(user, ADMINS);
    var granted = Instant.now();
    api.login(user);
    var latencies =
        Propagation.await(
            "admin granted, next login", granted, probes, true, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);

    stack.setRoles(user);
    var revoked = Instant.now();
    api.login(user);
    latencies =
        Propagation.await(
            "admin revoked, next login", revoked, probes, false, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);
  }

  /** The eng application, account and build service, as each service decides it. */
  private static Map<String, Callable<Boolean>> engAccess(String user) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    api.applicationRead(user, APP)
        .forEach((service, probe) -> probes.put("app read/" + service, probe));
    api.accountWrite(user, ACCOUNT_ENG)
        .forEach((service, probe) -> probes.put("account write/" + service, probe));
    api.buildServiceRead(user, JENKINS_ENG)
        .forEach((service, probe) -> probes.put("build service read/" + service, probe));
    return probes;
  }
}
