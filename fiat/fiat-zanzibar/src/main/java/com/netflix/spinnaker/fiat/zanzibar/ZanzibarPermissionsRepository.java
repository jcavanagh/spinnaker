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
import com.netflix.spinnaker.fiat.model.Authorization;
import com.netflix.spinnaker.fiat.model.UserPermission;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Resource;
import com.netflix.spinnaker.fiat.model.resources.Role;
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.permissions.PermissionsRepository;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * {@link PermissionsRepository} backed by the relationship store. {@link #get} builds a user's
 * permissions from concurrent store lookups; {@link #put} writes only the user's memberships.
 * Resource ACLs reach the store through change events and the bootstrap, never through this
 * repository.
 */
public class ZanzibarPermissionsRepository implements PermissionsRepository, AutoCloseable {

  private static final long DATA_CHECK_INTERVAL_MS = 30_000;

  private final ZanzibarStore store;
  private final List<ZanzibarResourceType> types;
  private final Reconciler reconciler;
  private final ReconcileLock lock;

  private final FiatAdminConfig adminConfig;
  private final AccountManagerConfig accountManagerConfig;

  /** Front50 service accounts; may be null. */
  private final ResourceProvider<ServiceAccount> serviceAccounts;

  private final ExecutorService lookupPool;

  private volatile long dataCheckedAt;
  private volatile boolean empty;

  public ZanzibarPermissionsRepository(
      ZanzibarStore store,
      List<ZanzibarResourceType> types,
      Reconciler reconciler,
      ReconcileLock lock,
      FiatZanzibarProperties properties,
      FiatAdminConfig adminConfig,
      AccountManagerConfig accountManagerConfig,
      ResourceProvider<ServiceAccount> serviceAccounts) {
    this.store = store;
    this.types = List.copyOf(types);
    this.reconciler = reconciler;
    this.lock = lock;
    this.adminConfig = adminConfig;
    this.accountManagerConfig = accountManagerConfig;
    this.serviceAccounts = serviceAccounts;
    var threads = new AtomicInteger();
    this.lookupPool =
        Executors.newFixedThreadPool(
            properties.getLookupParallelism(),
            r -> {
              var thread = new Thread(r, "zanzibar-lookup-" + threads.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
  }

  /** Replaces the user's memberships with {@code permission}'s roles. */
  @Override
  public PermissionsRepository put(UserPermission permission) {
    var userId = ZanzibarPrincipals.userId(permission.getId());
    var roles =
        ZanzibarPrincipals.roles(
            permission.getRoles().stream().map(Role::getName).collect(Collectors.toList()));
    lock.runExclusively("user:" + userId, () -> reconciler.reconcileUserMemberships(userId, roles));
    return this;
  }

  @Override
  public void putAllById(Map<String, UserPermission> permissions) {
    permissions.values().forEach(this::put);
  }

  @Override
  public Optional<UserPermission> get(String id) {
    var userId = ZanzibarPrincipals.userId(id);
    // Started first so the membership reads below overlap them.
    var lookups = startLookups(userId);

    var roles = new LinkedHashSet<Role>();
    for (var role : store.groupsOf(userId)) {
      roles.add(new Role(role));
    }
    var roleNames = roles.stream().map(Role::getName).collect(Collectors.toSet());
    // As DefaultPermissionsResolver: from the user's own roles.
    var admin = !Collections.disjoint(adminConfig.getAdmin().getRoles(), roleNames);

    var permission = new UserPermission().setId(userId);
    permission.setAdmin(admin);
    permission.setAccountManager(!Collections.disjoint(accountManagerConfig.getRoles(), roleNames));
    permission.setRoles(roles);

    // The View derives each resource's authorizations from the user's roles, so the store's answer
    // is carried on one of them. A user without roles reaches only unrestricted resources.
    var carrier = roles.isEmpty() ? null : roles.iterator().next().getName();
    lookups.forEach(
        (type, byAction) -> {
          var actionsByName = new LinkedHashMap<String, Permissions.Builder>();
          byAction.forEach(
              (action, names) -> {
                for (var name : names.join()) {
                  var builder = actionsByName.computeIfAbsent(name, n -> new Permissions.Builder());
                  if (carrier != null) {
                    builder.add(action, carrier);
                  }
                }
              });
          actionsByName.forEach(
              (name, builder) -> permission.addResource(type.newResource(name, builder.build())));
        });

    if (serviceAccounts != null) {
      permission.addResources(
          new ArrayList<Resource>(serviceAccounts.getAllRestricted(userId, roles, admin)));
    }
    return Optional.of(permission);
  }

  /** The store does not enumerate users; a full role sync then refreshes only service accounts. */
  @Override
  public Map<String, Set<Role>> getAllById() {
    return Map.of();
  }

  @Override
  public Map<String, Set<Role>> getAllByRoles(List<String> anyRoles) {
    return Map.of();
  }

  /** Memberships persist until the next login replaces them. */
  @Override
  public void remove(String id) {}

  /** True only when the store holds no relationships at all (for example, a wiped datastore). */
  @Override
  public boolean isEmpty() {
    var now = System.currentTimeMillis();
    if (now - dataCheckedAt >= DATA_CHECK_INTERVAL_MS) {
      empty = store.isEmpty();
      dataCheckedAt = now;
    }
    return empty;
  }

  @Override
  public void close() {
    lookupPool.shutdownNow();
  }

  /** One concurrent store lookup per (type, action). */
  private Map<ZanzibarResourceType, Map<Authorization, CompletableFuture<Set<String>>>>
      startLookups(String userId) {
    var lookups =
        new LinkedHashMap<
            ZanzibarResourceType, Map<Authorization, CompletableFuture<Set<String>>>>();
    for (var type : types) {
      var byAction =
          new EnumMap<Authorization, CompletableFuture<Set<String>>>(Authorization.class);
      for (var action : Authorization.values()) {
        byAction.put(
            action,
            CompletableFuture.supplyAsync(
                () ->
                    store.lookupResources(
                        userId, type.type().getName(), ZanzibarTypes.action(action)),
                lookupPool));
      }
      lookups.put(type, byAction);
    }
    return lookups;
  }
}
