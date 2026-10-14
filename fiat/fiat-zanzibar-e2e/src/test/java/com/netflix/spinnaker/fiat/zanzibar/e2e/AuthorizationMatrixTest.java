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

import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.createApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.json;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_MANAGERS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_OPEN;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_OPS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FRONT50;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.GATE;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.JENKINS_ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.JENKINS_OPEN;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.OPS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ROOT;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * One data set, and the decision every service makes on it: through the running services'
 * endpoints, and through each service's own checks in fiat-api.
 */
@ExtendWith(ZanzibarStackExtension.class)
@Order(1)
class AuthorizationMatrixTest {

  static final String ALICE = "alice@e2e.io";
  static final String BOB = "bob@e2e.io";
  static final String MALLORY = "mallory@e2e.io";
  static final String MANAGER = "manager@e2e.io";
  static final String NEWCOMER = "never-logged-in@e2e.io";

  /** A service account that is a member of {@code e2e-eng}. */
  static final String DEPLOYER = "deployer@e2e.io";

  static final List<String> USERS = List.of(ALICE, BOB, ROOT, MALLORY, DEPLOYER);

  static final String APP_OPEN = "e2ematrixopen";
  static final String APP_ENG = "e2ematrixeng";

  /** Readable by both groups; only eng writes, only ops executes. */
  static final String APP_SPLIT = "e2ematrixsplit";

  /** Never created. */
  static final String APP_UNKNOWN = "e2ematrixunknown";

  /** Who may do what, by resource type, resource and action: what every service must answer. */
  static final Map<String, Map<String, Map<String, Set<String>>>> EXPECTED = expected();

  static ZanzibarStack stack;
  static SpinnakerApi api;

  @BeforeAll
  static void createData(ZanzibarStack zanzibarStack) throws Exception {
    stack = zanzibarStack;
    api = new SpinnakerApi(stack);
    stack.setRoles(ALICE, ENG);
    stack.setRoles(BOB, OPS);
    stack.setRoles(MALLORY);
    stack.setRoles(MANAGER, ACCOUNT_MANAGERS);
    for (var user : List.of(ALICE, BOB, ROOT, MALLORY, MANAGER)) {
      api.login(user);
    }

    for (var app :
        Map.of(
                APP_OPEN,
                Map.<String, List<String>>of(),
                APP_ENG,
                Map.of("READ", List.of(ENG), "WRITE", List.of(ENG)),
                APP_SPLIT,
                Map.of("READ", List.of(ENG, OPS), "WRITE", List.of(ENG), "EXECUTE", List.of(OPS)))
            .entrySet()) {
      var task = api.runTask(ROOT, app.getKey(), createApplication(app.getKey(), app.getValue()));
      assertThat(task.path("status").asText()).as(app.getKey()).isEqualTo("SUCCEEDED");
    }
    json(
        api.send(
            "POST",
            FRONT50,
            "/serviceAccounts",
            ROOT,
            Map.of("name", DEPLOYER, "memberOf", List.of(ENG))));

    // Fiat lists service accounts from Front50 on its provider refresh.
    var ready = new LinkedHashMap<String, Callable<Boolean>>();
    ready.put("app", () -> api.authorizations(FIAT, ALICE, "applications", APP_SPLIT).size() == 2);
    ready.put("deployer roles", () -> api.roles(FIAT, DEPLOYER).contains(ENG));
    ready.put("deployer access", () -> api.serviceAccounts(FIAT, ALICE).contains(DEPLOYER));
    Propagation.await("matrix data in Fiat", Instant.now(), ready, true, Duration.ofSeconds(60));
  }

  static List<String> services() {
    return List.copyOf(ServiceChecks.CHECKS.keySet());
  }

  /** Each service's checks, made as that service makes them, for every user and resource. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("services")
  void serviceChecksMatchTheModel(String service) {
    var checks = new ServiceChecks(stack.url(FIAT));
    var mismatches = new ArrayList<String>();
    for (var check : ServiceChecks.CHECKS.get(service)) {
      var parts = check.split(":");
      EXPECTED
          .get(parts[0])
          .forEach(
              (resource, byAction) -> {
                for (var user : USERS) {
                  var expected = byAction.get(parts[1]).contains(user);
                  if (checks.allows(service, user, check, resource) != expected) {
                    mismatches.add(user + " " + check + " " + resource + ": expected " + expected);
                  }
                }
              });
    }
    assertThat(mismatches).as(service).isEmpty();
  }

  /** The same model, through the running services' own endpoints. */
  @Test
  void runningServicesMatchTheModel() {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    var expected = new LinkedHashMap<String, Boolean>();
    for (var user : USERS) {
      add(probes, expected, user, "APPLICATION", "READ", api::applicationRead);
      add(probes, expected, user, "APPLICATION", "WRITE", api::applicationWrite);
      add(probes, expected, user, "APPLICATION", "EXECUTE", api::applicationExecute);
      add(probes, expected, user, "ACCOUNT", "READ", api::accountRead);
      add(probes, expected, user, "ACCOUNT", "WRITE", api::accountWrite);
      add(probes, expected, user, "BUILD_SERVICE", "READ", api::buildServiceRead);
      add(probes, expected, user, "BUILD_SERVICE", "WRITE", api::buildServiceWrite);
      add(
          probes,
          expected,
          user,
          "SERVICE_ACCOUNT",
          "ACCESS",
          (u, serviceAccount) -> api.serviceAccountAccess(u, serviceAccount, APP_OPEN));
    }

    // Services may hold a View cached before the data existed, and Gate lists new applications
    // on its next refresh.
    var deadline = Instant.now().plus(Propagation.VIEW_CACHE_TTL).plus(Propagation.SLACK);
    List<String> mismatches;
    do {
      mismatches = new ArrayList<>();
      for (var probe : probes.entrySet()) {
        try {
          if (probe.getValue().call() != expected.get(probe.getKey())) {
            mismatches.add(probe.getKey() + ": expected " + expected.get(probe.getKey()));
          }
        } catch (Exception e) {
          mismatches.add(probe.getKey() + ": " + e.getMessage());
        }
      }
      if (!mismatches.isEmpty()) {
        SpinnakerApi.sleep(Duration.ofSeconds(1));
      }
    } while (!mismatches.isEmpty() && Instant.now().isBefore(deadline));
    assertThat(mismatches).isEmpty();
  }

  /** A principal that never logged in has no roles, so it reaches only unrestricted resources. */
  @Test
  void principalThatNeverLoggedInReachesOnlyUnrestrictedResources() {
    var view = api.view(FIAT, NEWCOMER);
    assertThat(view.path("roles")).isEmpty();
    assertThat(view.path("admin").asBoolean()).isFalse();
    assertThat(names(view, "applications")).contains(APP_OPEN).doesNotContain(APP_ENG, APP_SPLIT);
    assertThat(names(view, "accounts")).containsExactly(ACCOUNT_OPEN);
    assertThat(names(view, "buildServices")).containsExactly(JENKINS_OPEN);
    assertThat(names(view, "serviceAccounts")).isEmpty();
  }

  @Test
  void adminAndAccountManagerComeFromRoles() {
    assertThat(api.view(FIAT, ROOT).path("admin").asBoolean()).isTrue();
    assertThat(api.view(FIAT, MANAGER).path("accountManager").asBoolean()).isTrue();
    assertThat(api.view(FIAT, MANAGER).path("admin").asBoolean()).isFalse();
    assertThat(api.view(FIAT, ALICE).path("accountManager").asBoolean()).isFalse();
  }

  private static void add(
      Map<String, Callable<Boolean>> probes,
      Map<String, Boolean> expected,
      String user,
      String type,
      String action,
      BiFunction<String, String, Map<String, Callable<Boolean>>> probe) {
    EXPECTED
        .get(type)
        .forEach(
            (resource, byAction) -> {
              // Nothing lists an unknown application, so only fiat-api's checks cover it.
              if (resource.equals(APP_UNKNOWN)) {
                return;
              }
              probe
                  .apply(user, resource)
                  .forEach(
                      (service, decision) -> {
                        // A Gate login resolves roles as a person's, which would replace the
                        // service account's memberships.
                        if (service.equals(GATE) && user.equals(DEPLOYER)) {
                          return;
                        }
                        var name =
                            service + ": " + user + " " + type + ":" + action + " " + resource;
                        probes.put(name, decision);
                        expected.put(name, byAction.get(action).contains(user));
                      });
            });
  }

  private static List<String> names(JsonNode view, String field) {
    var names = new ArrayList<String>();
    view.path(field).forEach(node -> names.add(node.path("name").asText().toLowerCase()));
    return names;
  }

  private static Map<String, Map<String, Map<String, Set<String>>>> expected() {
    var everyone = Set.copyOf(USERS);
    var eng = Set.of(ALICE, ROOT, DEPLOYER);
    var engAndOps = Set.of(ALICE, BOB, ROOT, DEPLOYER);
    var ops = Set.of(BOB, ROOT);
    var admin = Set.of(ROOT);

    var applications = new LinkedHashMap<String, Map<String, Set<String>>>();
    applications.put(APP_OPEN, Map.of("READ", everyone, "WRITE", everyone, "EXECUTE", everyone));
    // EXECUTE falls back to READ when unset.
    applications.put(APP_ENG, Map.of("READ", eng, "WRITE", eng, "EXECUTE", eng));
    applications.put(APP_SPLIT, Map.of("READ", engAndOps, "WRITE", eng, "EXECUTE", ops));
    applications.put(APP_UNKNOWN, Map.of("READ", admin, "WRITE", admin, "EXECUTE", admin));

    var accounts = new LinkedHashMap<String, Map<String, Set<String>>>();
    accounts.put(ACCOUNT_OPEN, Map.of("READ", everyone, "WRITE", everyone));
    accounts.put(ACCOUNT_ENG, Map.of("READ", eng, "WRITE", eng));
    accounts.put(ACCOUNT_OPS, Map.of("READ", engAndOps, "WRITE", ops));

    var buildServices = new LinkedHashMap<String, Map<String, Set<String>>>();
    buildServices.put(JENKINS_OPEN, Map.of("READ", everyone, "WRITE", everyone));
    buildServices.put(JENKINS_ENG, Map.of("READ", eng, "WRITE", eng));

    return Map.of(
        "APPLICATION",
        applications,
        "ACCOUNT",
        accounts,
        "BUILD_SERVICE",
        buildServices,
        "SERVICE_ACCOUNT",
        Map.of(DEPLOYER, Map.of("ACCESS", eng)));
  }
}
