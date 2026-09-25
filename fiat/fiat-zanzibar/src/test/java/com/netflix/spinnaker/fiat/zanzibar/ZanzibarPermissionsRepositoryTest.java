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
import com.netflix.spinnaker.fiat.model.Authorization;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** Runs without Docker: the store's answers become the permissions Fiat serves. */
class ZanzibarPermissionsRepositoryTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<ZanzibarResourceType> TYPES =
      List.of(
          new AccountResourceType(Account::getPermissions, null, MAPPER),
          new ApplicationResourceType(
              Application::getPermissions,
              null,
              MAPPER,
              new DefaultFallbackPermissionsResolver(Authorization.EXECUTE, Authorization.READ),
              false),
          new BuildServiceResourceType(BuildService::getPermissions, null, MAPPER));

  @Test
  void buildsPermissionFromStore() {
    var adminConfig = new FiatAdminConfig();
    adminConfig.getAdmin().setRoles(List.of("admins"));
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            adminConfig,
            new AccountManagerConfig(),
            new FakeServiceAccounts());

    var view = repository.get("alice").orElseThrow().getView();

    assertThat(view.getApplications())
        .singleElement()
        .satisfies(
            app -> {
              assertThat(app.getName()).isEqualTo("app1");
              assertThat(app.getAuthorizations()).containsExactly(Authorization.READ);
            });
    assertThat(view.getAccounts())
        .singleElement()
        .satisfies(account -> assertThat(account.getAuthorizations()).hasSize(4));
    assertThat(view.getRoles()).extracting("name").containsExactlyInAnyOrder("eng", "ops");
    assertThat(view.getServiceAccounts()).extracting("name").containsExactly("sa1");
    assertThat(view.isAdmin()).isFalse();
    assertThat(view.isAccountManager()).isFalse();
  }

  @Test
  void adminAndAccountManagerComeFromTheConfiguredRoles() {
    var adminConfig = new FiatAdminConfig();
    adminConfig.getAdmin().setRoles(List.of("admins"));
    var accountManagerConfig = new AccountManagerConfig();
    accountManagerConfig.setRoles(List.of("ops"));
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            adminConfig,
            accountManagerConfig,
            null);

    var root = repository.get("ROOT").orElseThrow();
    var alice = repository.get("alice").orElseThrow();

    assertThat(root.isAdmin()).isTrue();
    assertThat(alice.isAdmin()).isFalse();
    assertThat(alice.isAccountManager()).isTrue();
  }

  @Test
  void serviceAccountsNeedEveryMemberOfRole() {
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            new FakeServiceAccounts());

    // alice holds eng and ops: sa1 (eng, ops) is usable; sa2 (eng, sre) is not.
    assertThat(repository.get("alice").orElseThrow().getServiceAccounts())
        .singleElement()
        .satisfies(sa -> assertThat(sa.getMemberOf()).containsExactlyInAnyOrder("eng", "ops"));
  }

  @Test
  void usersWithoutRolesSeeOnlyUnrestrictedResources() {
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            null);

    var view = repository.get("nobody").orElseThrow().getView();

    assertThat(view.getRoles()).isEmpty();
    assertThat(view.getApplications())
        .singleElement()
        .satisfies(
            app -> {
              assertThat(app.getName()).isEqualTo("public");
              assertThat(app.getAuthorizations()).hasSize(4);
            });
  }

  @Test
  void userIdsAreNormalizedAsStockFiatDoes() {
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            null);

    assertThat(repository.get("Alice@Example.com").orElseThrow().getId())
        .isEqualTo("alice@example.com");
    repository.get("anonymous");

    assertThat(store.userIds).containsOnly("alice@example.com", "__unrestricted_user__");
  }

  @Test
  void putWritesOnlyMemberships() {
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            null);
    var permission = new UserPermission().setId("Jane");
    permission.setRoles(Set.of(new Role("Eng"), new Role("ops")));

    repository.put(permission);

    assertThat(store.writes)
        .containsExactlyInAnyOrder(
            ZanzibarRelationship.member("eng", "user:jane"),
            ZanzibarRelationship.member("ops", "user:jane"));
  }

  @Test
  void theStoreEnumeratesNoUsersAndLogoutKeepsMemberships() {
    var store = new FakeStore();
    var repository =
        new ZanzibarPermissionsRepository(
            store,
            TYPES,
            new Reconciler(store),
            new InProcessReconcileLock(),
            new FiatZanzibarProperties(),
            new FiatAdminConfig(),
            new AccountManagerConfig(),
            null);

    repository.remove("alice");

    assertThat(repository.getAllById()).isEmpty();
    assertThat(repository.getAllByRoles(List.of("eng"))).isEmpty();
    assertThat(store.deletes).isEmpty();
  }

  @Test
  void isEmptyOnlyWhenTheStoreIsEmpty() {
    var empty = new FakeStore();
    var populated = new FakeStore();
    populated.empty = false;

    assertThat(
            new ZanzibarPermissionsRepository(
                    empty,
                    TYPES,
                    new Reconciler(empty),
                    new InProcessReconcileLock(),
                    new FiatZanzibarProperties(),
                    new FiatAdminConfig(),
                    new AccountManagerConfig(),
                    null)
                .isEmpty())
        .isTrue();
    assertThat(
            new ZanzibarPermissionsRepository(
                    populated,
                    TYPES,
                    new Reconciler(populated),
                    new InProcessReconcileLock(),
                    new FiatZanzibarProperties(),
                    new FiatAdminConfig(),
                    new AccountManagerConfig(),
                    null)
                .isEmpty())
        .isFalse();
  }

  /** Front50 service accounts: sa1 needs eng and ops, sa2 needs eng and sre. */
  private static final class FakeServiceAccounts extends BaseServiceAccountResourceProvider {
    FakeServiceAccounts() {
      super(List.of(new DefaultServiceAccountPredicateProvider(new FiatRoleConfig())));
    }

    @Override
    protected Set<ServiceAccount> loadAll() {
      return Set.of(serviceAccount("sa1", "eng", "ops"), serviceAccount("sa2", "eng", "sre"));
    }

    private static ServiceAccount serviceAccount(String name, String... memberOf) {
      var serviceAccount = new ServiceAccount();
      serviceAccount.setName(name);
      serviceAccount.setMemberOf(List.of(memberOf));
      return serviceAccount;
    }
  }

  /**
   * Store with canned answers: alice and root are in eng and ops (root also in admins), users read
   * app1 and has every action on acct1, and everyone has every action on the unrestricted "public"
   * application. Records what it is asked and written.
   */
  private static final class FakeStore implements ZanzibarStore {
    final Set<String> userIds = ConcurrentHashMap.newKeySet();
    final Set<ZanzibarRelationship> writes = ConcurrentHashMap.newKeySet();
    final Set<ZanzibarRelationship> deletes = ConcurrentHashMap.newKeySet();
    boolean empty = true;

    @Override
    public Set<String> lookupResources(
        String userId, String type, com.netflix.spinnaker.security.Authorization action) {
      userIds.add(userId);
      if ("nobody".equals(userId)) {
        return "application".equals(type) ? Set.of("public") : Set.of();
      }
      if ("application".equals(type)
          && action == com.netflix.spinnaker.security.Authorization.READ) {
        return Set.of("app1");
      }
      if ("account".equals(type)) {
        return Set.of("acct1");
      }
      return Set.of();
    }

    @Override
    public Set<String> groupsOf(String userId) {
      if ("root".equals(userId)) {
        return Set.of("eng", "ops", "admins");
      }
      return "alice".equals(userId) ? Set.of("eng", "ops") : Set.of();
    }

    @Override
    public void apply(
        Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
      this.writes.addAll(writes);
      this.deletes.addAll(deletes);
    }

    @Override
    public boolean isEmpty() {
      return empty;
    }

    @Override
    public boolean isMember(String userId, String groupId) {
      return groupsOf(userId).contains(groupId);
    }

    @Override
    public String providerId() {
      return "fake";
    }

    @Override
    public void applySchema() {}

    @Override
    public void write(Collection<ZanzibarRelationship> tuples) {}

    @Override
    public void delete(Collection<ZanzibarRelationship> tuples) {}

    @Override
    public Set<ZanzibarRelationship> read(String objectType, String objectId) {
      return Set.of();
    }

    @Override
    public Set<String> objectIds(String objectType) {
      return Set.of();
    }

    @Override
    public boolean check(
        String userId,
        String type,
        String name,
        com.netflix.spinnaker.security.Authorization action) {
      return false;
    }

    @Override
    public void close() {}
  }
}
