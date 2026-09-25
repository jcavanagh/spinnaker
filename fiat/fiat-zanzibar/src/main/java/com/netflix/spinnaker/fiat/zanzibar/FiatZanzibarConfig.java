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

import com.netflix.spinnaker.fiat.config.AccountManagerConfig;
import com.netflix.spinnaker.fiat.config.FiatAdminConfig;
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.permissions.PermissionsRepository;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import com.netflix.spinnaker.fiat.zanzibar.engine.ZanzibarEngine;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.CachingZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencyStrategy;
import com.netflix.spinnaker.kork.zanzibar.consistency.SharedConsistencyTokens;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Enables the store-backed Fiat authorization backend when {@code fiat.zanzibar.enabled=true}:
 * builds the store from the configured engine (cached) and makes {@link
 * ZanzibarPermissionsRepository} the primary {@link PermissionsRepository}, so Fiat answers from
 * the store. Other services are unaffected — they still call Fiat via {@code fiat-api}.
 *
 * <p>{@link FiatZanzibarEnvironmentPostProcessor} defaults the legacy repository and the periodic
 * role sync off, and {@link FiatZanzibarSyncConfig} bootstraps the store and receives change
 * events.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "fiat.zanzibar", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(FiatZanzibarProperties.class)
public class FiatZanzibarConfig {

  @Bean
  public ZanzibarSchema zanzibarSchema(List<ZanzibarResourceType> zanzibarResourceTypes) {
    return ZanzibarTypes.schema(zanzibarResourceTypes);
  }

  @Bean(destroyMethod = "close")
  public ZanzibarStore zanzibarStore(
      FiatZanzibarProperties properties,
      ZanzibarSchema zanzibarSchema,
      ObjectProvider<ZanzibarEngine> engines) {
    var engine = engines.getIfAvailable();
    if (engine == null) {
      throw new IllegalStateException("No Zanzibar engine named '" + properties.getEngine() + "'");
    }
    var store = engine.create(zanzibarSchema);
    // Provision the engine's schema, and its store where it has one, on startup.
    store.applySchema();
    return new CachingZanzibarStore(store, properties.getCacheTtl(), properties.getCacheMaxSize());
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  public ConsistencyStrategy zanzibarConsistencyStrategy(
      ZanzibarStore zanzibarStore,
      ObjectProvider<ZanzibarEngine> engines,
      ObjectProvider<SharedConsistencyTokens> shared) {
    return engines.getObject().consistencyStrategy(zanzibarStore, shared.getIfAvailable());
  }

  @Bean
  @ConditionalOnMissingBean
  public ReconcileLock zanzibarObjectLock() {
    return new InProcessReconcileLock();
  }

  @Bean
  public Reconciler zanzibarReconciler(ZanzibarStore zanzibarStore) {
    return new Reconciler(zanzibarStore);
  }

  @Bean(destroyMethod = "close")
  @Primary
  public ZanzibarPermissionsRepository zanzibarPermissionsRepository(
      ZanzibarStore zanzibarStore,
      List<ZanzibarResourceType> zanzibarResourceTypes,
      Reconciler zanzibarReconciler,
      ReconcileLock zanzibarObjectLock,
      FiatZanzibarProperties properties,
      FiatAdminConfig adminConfig,
      AccountManagerConfig accountManagerConfig,
      ObjectProvider<ResourceProvider<ServiceAccount>> serviceAccounts) {
    return new ZanzibarPermissionsRepository(
        zanzibarStore,
        zanzibarResourceTypes,
        zanzibarReconciler,
        zanzibarObjectLock,
        properties,
        adminConfig,
        accountManagerConfig,
        serviceAccounts.getIfAvailable());
  }
}
