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
import static com.netflix.spinnaker.fiat.zanzibar.e2e.SpinnakerApi.json;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ENG;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FRONT50;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.OPS;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ROOT;
import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The store is stateful: events lost in a Fiat outage, and a wiped datastore, are recovered by the
 * bootstrap and by logins. Runs last: it stops Fiat and wipes the store.
 */
@ExtendWith(ZanzibarStackExtension.class)
@Order(5)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StoreRecoveryTest {

  static final String OWNER = "recovery-owner@e2e.io";
  static final String READER = "recovery-reader@e2e.io";
  static final String APP = "e2erecovery";
  static final String SERVICE_ACCOUNT = "recovery-svc@e2e.io";

  /** How often Fiat checks whether the store holds any data. */
  static final Duration DATA_CHECK_INTERVAL = Duration.ofSeconds(30);

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
    json(
        api.send(
            "POST",
            FRONT50,
            "/serviceAccounts",
            ROOT,
            Map.of("name", SERVICE_ACCOUNT, "memberOf", List.of(ENG))));
  }

  /** Front50 drops an event after its retries when Fiat is down; the bootstrap applies it. */
  @Test
  @Order(1)
  void eventLostInFiatOutageIsRecoveredByBootstrap() throws Exception {
    var fiat = stack.service(FIAT);
    fiat.stop();
    try {
      json(
          api.send(
              "PUT",
              FRONT50,
              "/permissions/applications/" + APP,
              ROOT,
              Map.of(
                  "name",
                  APP,
                  "permissions",
                  Map.of("READ", List.of(ENG, OPS), "WRITE", List.of(ENG)))));
      // FiatResourceEvents retries three times with backoff, then drops the event.
      SpinnakerApi.sleep(Duration.ofSeconds(10));
    } finally {
      fiat.start();
      fiat.awaitHealthy(Duration.ofMinutes(5));
    }
    assertThat(readerCanRead().call()).as("lost event").isFalse();

    var result = api.bootstrap(FIAT);
    assertThat(result.path("resourceTuplesChanged").asLong()).isPositive();
    assertThat(readerCanRead().call()).as("after the bootstrap").isTrue();
  }

  /** An empty store means lost data, so Fiat answers 503 rather than deny every principal. */
  @Test
  @Order(2)
  void wipedStoreIsReportedUnavailable() {
    wipe(stack.store());
    Propagation.await(
        "wiped store reported unavailable",
        Instant.now(),
        Map.of(FIAT, () -> api.get(FIAT, "/authorize/" + OWNER, null).statusCode() == 503),
        true,
        DATA_CHECK_INTERVAL.plus(STORE_CACHE_TTL).plus(SLACK));
  }

  /** The bootstrap reloads resources and service accounts; people's memberships return at login. */
  @Test
  @Order(3)
  void bootstrapAndLoginsRebuildAWipedStore() throws Exception {
    wipe(stack.store());
    var result = api.bootstrap(FIAT);
    assertThat(result.path("resourceTuplesChanged").asLong()).isPositive();
    assertThat(api.roles(FIAT, SERVICE_ACCOUNT)).as("service account memberships").contains(ENG);

    Callable<Boolean> ownerCanRead =
        () -> api.authorizations(FIAT, OWNER, "applications", APP).contains("READ");
    assertThat(api.roles(FIAT, OWNER)).as("before the next login").isEmpty();
    assertThat(ownerCanRead.call()).as("before the next login").isFalse();

    var loggedIn = Instant.now();
    api.login(OWNER);
    Propagation.await(
        "wiped store, after bootstrap and login",
        loggedIn,
        Map.of(FIAT, ownerCanRead),
        true,
        SLACK);
  }

  private static Callable<Boolean> readerCanRead() {
    return () -> api.authorizations(FIAT, READER, "applications", APP).contains("READ");
  }

  private static void wipe(ZanzibarStore store) {
    for (var type : List.of("group", "account", "application", "build_service")) {
      for (var id : store.objectIds(type)) {
        store.delete(store.read(type, id));
      }
    }
    assertThat(store.isEmpty()).isTrue();
  }
}
