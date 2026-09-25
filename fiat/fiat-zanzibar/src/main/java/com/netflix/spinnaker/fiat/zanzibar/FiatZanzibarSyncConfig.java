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

package com.netflix.spinnaker.fiat.zanzibar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.ingest.ResourceChangeNotifier;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Store-population runtime, gated on {@code fiat.zanzibar.enabled}. {@link ZanzibarBootstrap} loads
 * the store from Fiat's authenticated {@link ResourceProvider} beans, and change events keep it
 * current after that.
 *
 * <p>Memberships are written by {@link ZanzibarPermissionsRepository} when Fiat stores a login or a
 * service-account sync, and by {@link ServiceAccountMembershipReconciler} at bootstrap.
 *
 * <p>{@code service_account} is not a stored type: its access derives from {@code memberOf} rather
 * than a permissions map.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "fiat.zanzibar", name = "enabled", havingValue = "true")
public class FiatZanzibarSyncConfig {

  @Bean
  public ServiceAccountMembershipReconciler zanzibarServiceAccountReconciler(
      ObjectProvider<ResourceProvider<ServiceAccount>> serviceAccounts,
      Reconciler zanzibarReconciler,
      ReconcileLock zanzibarObjectLock) {
    return new ServiceAccountMembershipReconciler(
        serviceAccounts.getIfAvailable(), zanzibarReconciler, zanzibarObjectLock);
  }

  @Bean
  public ZanzibarBootstrap zanzibarBootstrap(
      ZanzibarStore zanzibarStore,
      ReconcileLock zanzibarObjectLock,
      List<ZanzibarResourceType> zanzibarResourceTypes,
      ServiceAccountMembershipReconciler zanzibarServiceAccountReconciler) {
    return new ZanzibarBootstrap(
        zanzibarStore, zanzibarObjectLock, zanzibarResourceTypes, zanzibarServiceAccountReconciler);
  }

  @Bean
  public ResourceChangeNotifier zanzibarResourceChangeNotifier(
      Reconciler zanzibarReconciler, ReconcileLock zanzibarObjectLock) {
    return new ResourceChangeNotifier(zanzibarReconciler, zanzibarObjectLock, Runnable::run);
  }

  @Bean
  public ZanzibarResourcePermissions zanzibarResourcePermissions(
      ObjectMapper objectMapper, List<ZanzibarResourceType> zanzibarResourceTypes) {
    return new ZanzibarResourcePermissions(objectMapper, zanzibarResourceTypes);
  }
}
