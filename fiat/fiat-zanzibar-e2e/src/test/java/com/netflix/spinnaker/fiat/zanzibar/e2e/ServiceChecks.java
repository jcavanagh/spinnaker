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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spinnaker.fiat.shared.FiatClientConfigurationProperties;
import com.netflix.spinnaker.fiat.shared.FiatPermissionEvaluator;
import com.netflix.spinnaker.fiat.shared.FiatService;
import com.netflix.spinnaker.fiat.shared.FiatStatus;
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService;
import com.netflix.spinnaker.kork.retrofit.ErrorHandlingExecutorCallAdapterFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * Each service's authorization checks, made through fiat-api's {@link FiatPermissionEvaluator} —
 * the client every service uses — with one evaluator, and so one View cache, per service.
 */
public class ServiceChecks {

  /** The {@code type:action} checks each service makes, from its controllers and tasks. */
  public static final Map<String, List<String>> CHECKS = checks();

  private final Map<String, FiatPermissionEvaluator> evaluators = new LinkedHashMap<>();

  public ServiceChecks(String fiatUrl) {
    // As FiatAuthenticationConfig builds it.
    var mapper =
        new ObjectMapper()
            .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    var fiatService =
        new Retrofit.Builder()
            .baseUrl(fiatUrl + "/")
            .client(new OkHttpClient())
            .addCallAdapterFactory(ErrorHandlingExecutorCallAdapterFactory.getInstance())
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .build()
            .create(FiatService.class);
    var properties = new FiatClientConfigurationProperties();
    properties.setEnabled(true);
    properties.setBaseUrl(fiatUrl);
    var registry = new NoopRegistry();
    var status = new FiatStatus(registry, DynamicConfigService.NOOP, properties);
    CHECKS
        .keySet()
        .forEach(
            service ->
                evaluators.put(
                    service,
                    new FiatPermissionEvaluator(registry, fiatService, properties, status)));
  }

  /**
   * {@code service}'s decision; {@code check} is {@code type:action}, e.g. {@code
   * APPLICATION:READ}.
   */
  public boolean allows(String service, String user, String check, String resource) {
    var parts = check.split(":");
    return evaluators.get(service).hasPermission(user, resource, parts[0], parts[1]);
  }

  private static Map<String, List<String>> checks() {
    var checks = new LinkedHashMap<String, List<String>>();
    // Application list filter, plugin info writes, pipeline triggers, deployable accounts.
    checks.put(
        "gate",
        List.of("APPLICATION:READ", "APPLICATION:WRITE", "APPLICATION:EXECUTE", "ACCOUNT:WRITE"));
    // Application reads and writes, pipeline runAsUser.
    checks.put(
        "front50", List.of("APPLICATION:READ", "APPLICATION:WRITE", "SERVICE_ACCOUNT:ACCESS"));
    // Cluster and job reads, run job, account reads and operations.
    checks.put(
        "clouddriver",
        List.of("APPLICATION:READ", "APPLICATION:EXECUTE", "ACCOUNT:READ", "ACCOUNT:WRITE"));
    // Execution reads, cancels, and restarts.
    checks.put("orca", List.of("APPLICATION:READ", "APPLICATION:WRITE", "APPLICATION:EXECUTE"));
    // Build masters and jobs.
    checks.put("igor", List.of("BUILD_SERVICE:READ", "BUILD_SERVICE:WRITE"));
    // Triggers run as their runAsUser.
    checks.put("echo", List.of("APPLICATION:EXECUTE"));
    // Delivery configs, their accounts, and their service account.
    checks.put(
        "keel",
        List.of(
            "APPLICATION:READ",
            "APPLICATION:WRITE",
            "ACCOUNT:READ",
            "ACCOUNT:WRITE",
            "SERVICE_ACCOUNT:ACCESS"));
    return checks;
  }
}
