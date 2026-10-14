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
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.deleteApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.json;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.kubernetesAccount;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.updateApplication;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ACCOUNT_MANAGERS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.CLOUDDRIVER;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FRONT50;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.GATE;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.OPS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ROOT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
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
 * Resources change where they are authored — applications through Gate and Orca into Front50,
 * accounts through Gate into Clouddriver, service accounts in Front50 — and reach Fiat as events.
 * Each test measures how long a change takes to reach every service.
 */
@ExtendWith(ZanzibarStackExtension.class)
@Order(3)
class ResourcePropagationTest {

  static final String OWNER = "owner@e2e.io";
  static final String READER = "reader@e2e.io";
  static final String MANAGER = "account-manager@e2e.io";

  static final Map<String, List<String>> ENG_ONLY =
      Map.of("READ", List.of(ENG), "WRITE", List.of(ENG));
  static final Map<String, List<String>> ENG_AND_OPS_READ =
      Map.of("READ", List.of(ENG, OPS), "WRITE", List.of(ENG));

  /** Fiat lists service accounts from Front50 on its provider refresh, 15 seconds by default. */
  static final Duration SERVICE_ACCOUNT_LISTING = Duration.ofSeconds(60);

  static ZanzibarStack stack;
  static SpinnakerApi api;

  @BeforeAll
  static void createUsers(ZanzibarStack zanzibarStack) {
    stack = zanzibarStack;
    api = new SpinnakerApi(stack);
    stack.setRoles(OWNER, ENG);
    stack.setRoles(READER, OPS);
    stack.setRoles(MANAGER, ACCOUNT_MANAGERS, ENG);
    List.of(OWNER, READER, MANAGER).forEach(api::login);
  }

  @Test
  void applicationCreatedThroughGateAndOrca() throws Exception {
    var app = "e2ecreated";
    runTask(OWNER, app, createApplication(app, ENG_ONLY));
    var created = Instant.now();
    var latencies =
        Propagation.await(
            "application created",
            created,
            readAndWrite(OWNER, app),
            true,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, "read/" + FIAT, "write/" + FIAT);
    Propagation.assertNow("other group", api.applicationRead(READER, app), false);
  }

  @Test
  void permissionGrantAndRevokePropagate() throws Exception {
    var app = "e2egrant";
    runTask(OWNER, app, createApplication(app, ENG_ONLY));
    Propagation.await(
        "owner reads new application",
        Instant.now(),
        api.applicationRead(OWNER, app),
        true,
        VIEW_CACHE_TTL.plus(SLACK));

    var reader = api.applicationRead(READER, app);
    Propagation.assertNow("before the grant", reader, false);
    runTask(OWNER, app, updateApplication(app, ENG_AND_OPS_READ));
    var granted = Instant.now();
    var latencies =
        Propagation.await(
            "application READ granted to a group",
            granted,
            reader,
            true,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);

    runTask(OWNER, app, updateApplication(app, ENG_ONLY));
    var revoked = Instant.now();
    latencies =
        Propagation.await(
            "application READ revoked from a group",
            revoked,
            reader,
            false,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);
  }

  /** An unrestricted application is open to everyone until its first restriction. */
  @Test
  void restrictingAnOpenApplication() throws Exception {
    var app = "e2erestricted";
    runTask(OWNER, app, createApplication(app, Map.of()));
    var reader = api.applicationRead(READER, app);
    Propagation.await("open application", Instant.now(), reader, true, VIEW_CACHE_TTL.plus(SLACK));

    runTask(OWNER, app, updateApplication(app, ENG_ONLY));
    var restricted = Instant.now();
    var latencies =
        Propagation.await(
            "open application restricted", restricted, reader, false, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);
    Propagation.assertNow("owner group", api.applicationRead(OWNER, app), true);
  }

  @Test
  void deletedApplicationLeavesTheStore() {
    var app = "e2edeleted";
    runTask(OWNER, app, createApplication(app, ENG_ONLY));
    // Deleting checks the application in Clouddriver, which denies it until its cached View for
    // the owner expires.
    Propagation.await(
        "owner reads new application",
        Instant.now(),
        api.applicationRead(OWNER, app),
        true,
        VIEW_CACHE_TTL.plus(SLACK));
    // Front50 names applications in upper case.
    assertThat(stack.store().read("application", app.toUpperCase())).isNotEmpty();

    runTask(OWNER, app, deleteApplication(app));
    var deleted = Instant.now();
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, api.applicationRead(OWNER, app).get(FIAT));
    probes.put("store", () -> !stack.store().read("application", app.toUpperCase()).isEmpty());
    Propagation.await("application deleted", deleted, probes, false, SLACK);
  }

  /** A burst of writes ends at the last write, in Fiat and in the store. */
  @Test
  void burstOfWritesConvergesOnTheLast() throws Exception {
    var app = "e2eburst";
    runTask(OWNER, app, createApplication(app, ENG_ONLY));
    for (var i = 0; i < 10; i++) {
      json(
          api.send(
              "PUT",
              FRONT50,
              "/permissions/applications/" + app,
              OWNER,
              Map.of("name", app, "permissions", i % 2 == 0 ? ENG_ONLY : ENG_AND_OPS_READ)));
    }
    var written = Instant.now();
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(
        "reader/" + FIAT,
        () -> api.authorizations(FIAT, READER, "applications", app).contains("READ"));
    probes.put(
        "store",
        () ->
            stack.store().read("application", app.toUpperCase()).stream()
                .anyMatch(tuple -> tuple.getSubjectRef().equals("group:" + OPS)));
    Propagation.await("burst of 10 writes", written, probes, true, SLACK);
    // An earlier write applied late would undo the last one.
    SpinnakerApi.sleep(SLACK);
    Propagation.assertNow("after the burst", probes, true);
  }

  @Test
  void accountLifecycleThroughClouddriver() throws Exception {
    var account = "e2e-k8s-managed";
    json(api.send("POST", GATE, "/credentials", MANAGER, kubernetesAccount(account, ENG_ONLY)));
    var created = Instant.now();
    var latencies =
        Propagation.await(
            "account created",
            created,
            api.accountWrite(OWNER, account),
            true,
            VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE);

    // Clouddriver updates only accounts it has loaded, for a manager that may write them.
    Propagation.await(
        "account loaded",
        Instant.now(),
        Map.of(
            "loaded",
            () -> api.get(CLOUDDRIVER, "/credentials/" + account, MANAGER).statusCode() == 200,
            "manager",
            api.accountWrite(MANAGER, account).get(CLOUDDRIVER)),
        true,
        VIEW_CACHE_TTL.plus(SLACK));

    var reader = api.accountRead(READER, account);
    Propagation.assertNow("before the grant", reader, false);
    json(
        api.send(
            "PUT", GATE, "/credentials", MANAGER, kubernetesAccount(account, ENG_AND_OPS_READ)));
    var granted = Instant.now();
    latencies =
        Propagation.await(
            "account READ granted to a group", granted, reader, true, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT);

    json(api.send("DELETE", GATE, "/credentials/" + account, MANAGER, null));
    var deleted = Instant.now();
    var probes = new LinkedHashMap<String, Callable<Boolean>>(api.accountWrite(OWNER, account));
    probes.put("store", () -> !stack.store().read("account", account).isEmpty());
    latencies =
        Propagation.await("account deleted", deleted, probes, false, VIEW_CACHE_TTL.plus(SLACK));
    Propagation.assertWithin(latencies, SLACK, FIAT, GATE, "store");
  }

  /**
   * A service account's memberships come from its Front50 {@code memberOf}; users whose roles cover
   * it may run pipelines as it.
   */
  @Test
  void serviceAccountLifecycleThroughFront50() throws Exception {
    var serviceAccount = "svc-ops@e2e.io";
    var opsApp = "e2eopsonly";
    var openApp = "e2esvcopen";
    runTask(
        ROOT,
        opsApp,
        createApplication(opsApp, Map.of("READ", List.of(OPS), "WRITE", List.of(OPS))));
    runTask(ROOT, openApp, createApplication(openApp, Map.of()));
    var checks = new ServiceChecks(stack.url(FIAT));

    var principal = new LinkedHashMap<String, Callable<Boolean>>();
    principal.put("roles/" + FIAT, () -> api.roles(FIAT, serviceAccount).contains(OPS));
    // Echo triggers a pipeline only when its runAsUser may execute the application.
    principal.put(
        "execute/" + FIAT,
        () -> api.authorizations(FIAT, serviceAccount, "applications", opsApp).contains("EXECUTE"));
    principal.put(
        "execute/echo", () -> checks.allows("echo", serviceAccount, "APPLICATION:EXECUTE", opsApp));
    var access = api.serviceAccountAccess(READER, serviceAccount, openApp);
    Propagation.assertNow("before the service account exists", access, false);

    json(
        api.send(
            "POST",
            FRONT50,
            "/serviceAccounts",
            ROOT,
            Map.of("name", serviceAccount, "memberOf", List.of(OPS))));
    var created = Instant.now();
    var latencies = Propagation.await("service account created", created, principal, true, SLACK);
    Propagation.assertWithin(latencies, SLACK, "roles/" + FIAT);
    Propagation.await(
        "service account usable by its group", created, access, true, SERVICE_ACCOUNT_LISTING);

    json(api.send("DELETE", FRONT50, "/serviceAccounts/" + serviceAccount, ROOT, null));
    var deleted = Instant.now();
    var removed = new LinkedHashMap<String, Callable<Boolean>>();
    removed.put("roles/" + FIAT, principal.get("roles/" + FIAT));
    removed.put("execute/" + FIAT, principal.get("execute/" + FIAT));
    Propagation.await("service account deleted", deleted, removed, false, SLACK);
    Propagation.await(
        "service account no longer usable", deleted, access, false, SERVICE_ACCOUNT_LISTING);
  }

  private static void runTask(String user, String app, Map<String, Object> job) {
    var task = api.runTask(user, app, job);
    assertThat(task.path("status").asText())
        .as("%s %s", job.get("type"), app)
        .isEqualTo("SUCCEEDED");
  }

  private static Map<String, Callable<Boolean>> readAndWrite(String user, String app) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    api.applicationRead(user, app)
        .forEach((service, probe) -> probes.put("read/" + service, probe));
    api.applicationWrite(user, app)
        .forEach((service, probe) -> probes.put("write/" + service, probe));
    return probes;
  }
}
