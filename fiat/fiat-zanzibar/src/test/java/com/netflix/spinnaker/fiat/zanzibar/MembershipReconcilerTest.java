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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.config.AccountManagerConfig;
import com.netflix.spinnaker.fiat.config.FiatAdminConfig;
import com.netflix.spinnaker.fiat.config.FiatRoleConfig;
import com.netflix.spinnaker.fiat.model.UserPermission;
import com.netflix.spinnaker.fiat.model.resources.Account;
import com.netflix.spinnaker.fiat.model.resources.Application;
import com.netflix.spinnaker.fiat.model.resources.BuildService;
import com.netflix.spinnaker.fiat.model.resources.Role;
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.permissions.DefaultFallbackPermissionsResolver;
import com.netflix.spinnaker.fiat.providers.BaseServiceAccountResourceProvider;
import com.netflix.spinnaker.fiat.providers.DefaultServiceAccountPredicateProvider;
import com.netflix.spinnaker.fiat.zanzibar.resource.AccountResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ApplicationResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.BuildServiceResourceType;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarRelationship;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.security.Authorization;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Runs without Docker: login and service-account writes store normalized memberships. */
class MembershipReconcilerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<ZanzibarResourceType> TYPES =
      List.of(
          new AccountResourceType(Account::getPermissions, null, MAPPER),
          new ApplicationResourceType(
              Application::getPermissions,
              null,
              MAPPER,
              new DefaultFallbackPermissionsResolver(
                  com.netflix.spinnaker.fiat.model.Authorization.EXECUTE,
                  com.netflix.spinnaker.fiat.model.Authorization.READ),
              false),
          new BuildServiceResourceType(BuildService::getPermissions, null, MAPPER));

  @Test
  void loginNormalizesUserAndRoles() {
    var store = new MembershipStore();

    repository(store)
        .put(permission("Jane.Doe@Example.com", " Eng ", "OPS", "Jane.Doe@Example.com"));

    assertThat(store.groupsOf("jane.doe@example.com"))
        .containsExactlyInAnyOrder("eng", "ops", "jane.doe@example.com");
  }

  @Test
  void loginReplacesPreviousMemberships() {
    var store = new MembershipStore();
    var repository = repository(store);

    repository.put(permission("jane", "eng", "ops"));
    repository.put(permission("jane", "eng"));

    assertThat(store.groupsOf("jane")).containsExactly("eng");
  }

  @Test
  void urnStylePrincipalsAreValid() {
    var store = new MembershipStore();

    repository(store).put(permission("urn:example:person:1234567", "7654321"));

    assertThat(store.groupsOf("urn:example:person:1234567")).containsExactly("7654321");
  }

  @Test
  void anonymousMapsToTheUnrestrictedUser() {
    var store = new MembershipStore();

    repository(store).put(permission("Anonymous", "eng"));

    assertThat(store.groupsOf("__unrestricted_user__")).containsExactly("eng");
    assertThat(store.groupsOf("anonymous")).isEmpty();
  }

  @Test
  void serviceAccountsGetTheirMemberOfAndLoseItOnDelete() {
    var store = new MembershipStore();
    var serviceAccount = new ServiceAccount();
    serviceAccount.setName("Svc@managed-service-account");
    serviceAccount.setMemberOf(List.of("Eng", "ops"));
    var provider =
        new BaseServiceAccountResourceProvider(
            List.of(new DefaultServiceAccountPredicateProvider(new FiatRoleConfig()))) {
          @Override
          protected Set<ServiceAccount> loadAll() {
            return Set.of(serviceAccount);
          }
        };
    var reconciler =
        new ServiceAccountMembershipReconciler(
            provider, new Reconciler(store), new InProcessReconcileLock());

    assertThat(reconciler.reconcileAll(new Reconciler(store))).isEqualTo(2);
    assertThat(store.groupsOf("svc@managed-service-account"))
        .containsExactlyInAnyOrder("eng", "ops");
    assertThat(reconciler.reconcileAll(new Reconciler(store))).isZero();

    reconciler.reconcile("svc@managed-service-account", List.of());
    assertThat(store.groupsOf("svc@managed-service-account")).isEmpty();
  }

  @Test
  void serviceAccountReconcileAllWithoutAProviderDoesNothing() {
    var store = new MembershipStore();
    var reconciler =
        new ServiceAccountMembershipReconciler(
            null, new Reconciler(store), new InProcessReconcileLock());

    assertThat(reconciler.reconcileAll(new Reconciler(store))).isZero();
  }

  private static ZanzibarPermissionsRepository repository(MembershipStore store) {
    return new ZanzibarPermissionsRepository(
        store,
        TYPES,
        new Reconciler(store),
        new InProcessReconcileLock(),
        new FiatZanzibarProperties(),
        new FiatAdminConfig(),
        new AccountManagerConfig(),
        null);
  }

  private static UserPermission permission(String id, String... roles) {
    var permission = new UserPermission().setId(id);
    var set = new LinkedHashSet<Role>();
    for (var role : roles) {
      set.add(new Role(role));
    }
    permission.setRoles(set);
    return permission;
  }

  /** Keeps membership edges in memory so reconciles are observable. */
  private static final class MembershipStore implements ZanzibarStore {
    private final Map<String, Set<String>> groupsByUser = new HashMap<>();

    @Override
    public Set<String> groupsOf(String userId) {
      return new LinkedHashSet<>(groupsByUser.getOrDefault(userId, Set.of()));
    }

    @Override
    public void apply(
        Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
      for (var tuple : deletes) {
        groupsByUser.getOrDefault(user(tuple), new LinkedHashSet<>()).remove(tuple.getObjectId());
      }
      for (var tuple : writes) {
        groupsByUser
            .computeIfAbsent(user(tuple), u -> new LinkedHashSet<>())
            .add(tuple.getObjectId());
      }
    }

    private static String user(ZanzibarRelationship tuple) {
      return tuple.getSubjectRef().substring("user:".length());
    }

    @Override
    public String providerId() {
      return "memory";
    }

    @Override
    public boolean isEmpty() {
      return groupsByUser.isEmpty();
    }

    @Override
    public void applySchema() {}

    @Override
    public void write(Collection<ZanzibarRelationship> tuples) {
      apply(tuples, List.of());
    }

    @Override
    public void delete(Collection<ZanzibarRelationship> tuples) {
      apply(List.of(), tuples);
    }

    @Override
    public Set<ZanzibarRelationship> read(String objectType, String objectId) {
      return Set.of();
    }

    @Override
    public Set<String> objectIds(String objectType) {
      return Set.of();
    }

    @Override
    public boolean check(String userId, String type, String name, Authorization action) {
      return false;
    }

    @Override
    public boolean isMember(String userId, String groupId) {
      return groupsOf(userId).contains(groupId);
    }

    @Override
    public Set<String> lookupResources(String userId, String type, Authorization action) {
      return Set.of();
    }

    @Override
    public void close() {}
  }
}
