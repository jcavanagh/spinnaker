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

import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.CLOUDDRIVER;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FIAT;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.FRONT50;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.GATE;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.IGOR;
import static com.netflix.spinnaker.fiat.zanzibar.e2e.ZanzibarStack.ORCA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;

/**
 * Calls the stack's services as a user, through {@code X-SPINNAKER-USER}, and reads each service's
 * authorization decision from its response.
 */
@RequiredArgsConstructor
public class SpinnakerApi {

  public static final ObjectMapper JSON = new ObjectMapper();

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  private final ZanzibarStack stack;

  public HttpResponse<String> get(String service, String path, String user) {
    return send("GET", service, path, user, null);
  }

  public HttpResponse<String> send(
      String method, String service, String path, String user, Object body) {
    try {
      var request =
          HttpRequest.newBuilder(URI.create(stack.url(service) + path))
              .timeout(Duration.ofSeconds(60))
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .method(
                  method,
                  body == null
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
      if (user != null) {
        request.header("X-SPINNAKER-USER", user);
      }
      return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new IllegalStateException(method + " " + service + path + " failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** The body of a 2xx response; fails otherwise. */
  public static JsonNode json(HttpResponse<String> response) {
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(describe(response));
    }
    try {
      return response.body().isEmpty() ? JSON.createObjectNode() : JSON.readTree(response.body());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(describe(response), e);
    }
  }

  public static String describe(HttpResponse<String> response) {
    return response.request().method()
        + " "
        + response.request().uri()
        + " -> "
        + response.statusCode()
        + " "
        + response.body();
  }

  // --- Fiat -----------------------------------------------------------------------------------

  /** The user's whole permission, as every service's {@code FiatPermissionEvaluator} reads it. */
  public JsonNode view(String fiat, String user) {
    return json(get(fiat, "/authorize/" + user, null));
  }

  /**
   * The user's authorizations on one resource; {@code collection} is the View's field, e.g. {@code
   * applications}, {@code accounts} or {@code buildServices}.
   */
  public Set<String> authorizations(String fiat, String user, String collection, String name) {
    for (var resource : view(fiat, user).path(collection)) {
      if (resource.path("name").asText().equalsIgnoreCase(name)) {
        return texts(resource.path("authorizations"));
      }
    }
    return Set.of();
  }

  public Set<String> roles(String fiat, String user) {
    return names(view(fiat, user).path("roles"));
  }

  public Set<String> serviceAccounts(String fiat, String user) {
    return names(view(fiat, user).path("serviceAccounts"));
  }

  /** Runs the store bootstrap on {@code fiat}, as an admin does after a deploy or an outage. */
  public JsonNode bootstrap(String fiat) {
    return json(send("POST", fiat, "/zanzibar/bootstrap", null, null));
  }

  // --- Gate -----------------------------------------------------------------------------------

  /** Logs in through Gate: header authentication resolves the user's roles in Fiat. */
  public void login(String user) {
    json(get(GATE, "/auth/user", user));
  }

  /** Submits a task through Gate, as Deck does, and waits for Orca to finish it. */
  public JsonNode runTask(String user, String application, Map<String, Object> job) {
    var task =
        Map.of(
            "application",
            application,
            "description",
            job.get("type") + " " + application,
            "job",
            List.of(job));
    var ref =
        json(send("POST", GATE, "/applications/" + application + "/tasks", user, task))
            .path("ref")
            .asText();
    var deadline = Instant.now().plus(Duration.ofMinutes(2));
    while (Instant.now().isBefore(deadline)) {
      var status = json(get(GATE, ref, user));
      if (status.path("status").asText().matches("SUCCEEDED|TERMINAL|CANCELED|STOPPED")) {
        return status;
      }
      sleep(Duration.ofMillis(250));
    }
    throw new IllegalStateException("Task " + ref + " did not finish");
  }

  /** Orca's {@code createApplication} job; {@code permissions} maps actions to roles. */
  public static Map<String, Object> createApplication(
      String application, Map<String, List<String>> permissions) {
    return applicationJob("createApplication", application, permissions);
  }

  /** Orca's {@code updateApplication} job. */
  public static Map<String, Object> updateApplication(
      String application, Map<String, List<String>> permissions) {
    return applicationJob("updateApplication", application, permissions);
  }

  public static Map<String, Object> deleteApplication(String application) {
    return Map.of("type", "deleteApplication", "application", Map.of("name", application));
  }

  private static Map<String, Object> applicationJob(
      String type, String application, Map<String, List<String>> permissions) {
    var model = new LinkedHashMap<String, Object>();
    model.put("name", application);
    model.put("email", "e2e@example.com");
    model.put("cloudProviders", "kubernetes");
    model.put("permissions", permissions);
    return Map.of("type", type, "application", model);
  }

  /** A Kubernetes account definition, as Clouddriver's account management API takes it. */
  public static Map<String, Object> kubernetesAccount(
      String name, Map<String, List<String>> permissions) {
    var account = new LinkedHashMap<String, Object>();
    account.put("type", "kubernetes");
    account.put("name", name);
    account.put(
        "kubeconfigFile", System.getProperty("zanzibar.e2e.workDir") + "/config/kubeconfig.yml");
    account.put("namespaces", List.of("default"));
    account.put("checkPermissionsOnStartup", false);
    account.put("cacheIntervalSeconds", 3600);
    account.put("permissions", permissions);
    return account;
  }

  // --- Decisions, per service -----------------------------------------------------------------
  //
  // Each probe returns the service's decision: true when allowed, false when denied. A probe
  // throws on a response that is neither, so a broken service is not mistaken for a denial.

  public Map<String, Callable<Boolean>> applicationRead(String user, String application) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "applications", application, "READ"));
    probes.put(
        GATE, () -> names(json(get(GATE, "/applications", user))).contains(lower(application)));
    probes.put(FRONT50, () -> decision(get(FRONT50, "/v2/applications/" + application, user), 200));
    probes.put(
        CLOUDDRIVER,
        () -> decision(get(CLOUDDRIVER, "/applications/" + application, user), 200, 404));
    probes.put(
        ORCA, () -> decision(get(ORCA, "/applications/" + application + "/tasks", user), 200));
    return probes;
  }

  public Map<String, Callable<Boolean>> applicationWrite(String user, String application) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "applications", application, "WRITE"));
    // Deleting a pipeline that does not exist: Front50 checks WRITE first.
    probes.put(
        FRONT50,
        () ->
            decision(
                send("DELETE", FRONT50, "/pipelines/" + application + "/e2e-none", user, null),
                200,
                404));
    return probes;
  }

  public Map<String, Callable<Boolean>> applicationExecute(String user, String application) {
    return Map.of(FIAT, () -> fiatAllows(FIAT, user, "applications", application, "EXECUTE"));
  }

  public Map<String, Callable<Boolean>> accountRead(String user, String account) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "accounts", account, "READ"));
    probes.put(
        CLOUDDRIVER,
        () -> decision(get(CLOUDDRIVER, "/securityGroups/" + account, user), 200, 404));
    return probes;
  }

  public Map<String, Callable<Boolean>> accountWrite(String user, String account) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "accounts", account, "WRITE"));
    // Gate lists the accounts the user may deploy to.
    probes.put(
        GATE,
        () -> texts(json(get(GATE, "/auth/user", user)).path("allowedAccounts")).contains(account));
    probes.put(
        CLOUDDRIVER,
        () ->
            decision(get(CLOUDDRIVER, "/credentials/" + account + "/authorized", user), 200, 404));
    return probes;
  }

  public Map<String, Callable<Boolean>> buildServiceRead(String user, String master) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "buildServices", master, "READ"));
    probes.put(IGOR, () -> texts(json(get(IGOR, "/masters", user))).contains(master));
    return probes;
  }

  public Map<String, Callable<Boolean>> buildServiceWrite(String user, String master) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> fiatAllows(FIAT, user, "buildServices", master, "WRITE"));
    // Stopping a build on a master that does not answer: Igor checks WRITE first.
    probes.put(
        IGOR,
        () ->
            send("PUT", IGOR, "/masters/" + master + "/jobs/stop/1/1?jobName=e2e", user, null)
                    .statusCode()
                != 403);
    return probes;
  }

  public Map<String, Callable<Boolean>> serviceAccountAccess(
      String user, String serviceAccount, String application) {
    var probes = new LinkedHashMap<String, Callable<Boolean>>();
    probes.put(FIAT, () -> serviceAccounts(FIAT, user).contains(lower(serviceAccount)));
    // Saving a pipeline that runs as the service account; the user needs WRITE on the app. The
    // fixed id makes repeated saves updates.
    var id = UUID.nameUUIDFromBytes((application + "/" + user).getBytes()).toString();
    probes.put(
        FRONT50,
        () ->
            decision(
                send(
                    "POST",
                    FRONT50,
                    "/pipelines",
                    user,
                    Map.of(
                        "id",
                        id,
                        "application",
                        application,
                        "name",
                        "runs-as-" + id,
                        "triggers",
                        List.of(
                            Map.of(
                                "type",
                                "cron",
                                "enabled",
                                false,
                                "cronExpression",
                                "0 0 0 1 1 ? 2099",
                                "runAsUser",
                                serviceAccount)))),
                200));
    return probes;
  }

  private boolean fiatAllows(
      String fiat, String user, String collection, String name, String action) {
    return authorizations(fiat, user, collection, name).contains(action);
  }

  /** True on an allowed status, false on 403; throws on anything else. */
  private static boolean decision(HttpResponse<String> response, int... allowed) {
    if (response.statusCode() == 403) {
      return false;
    }
    for (var status : allowed) {
      if (response.statusCode() == status) {
        return true;
      }
    }
    throw new IllegalStateException("Unexpected " + describe(response));
  }

  private static Set<String> names(JsonNode nodes) {
    var names = new TreeSet<String>();
    nodes.forEach(node -> names.add(lower(node.path("name").asText())));
    return names;
  }

  private static Set<String> texts(JsonNode nodes) {
    var texts = new TreeSet<String>();
    nodes.forEach(node -> texts.add(node.asText()));
    return texts;
  }

  private static String lower(String value) {
    return value.toLowerCase();
  }

  static void sleep(Duration duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
