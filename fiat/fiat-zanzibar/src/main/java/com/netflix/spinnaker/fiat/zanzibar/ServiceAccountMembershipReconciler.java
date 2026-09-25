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

import com.netflix.spinnaker.fiat.model.resources.ServiceAccount;
import com.netflix.spinnaker.fiat.providers.ResourceProvider;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import java.util.Collection;
import lombok.RequiredArgsConstructor;

/**
 * Keeps service-account principals' memberships equal to their Front50 {@code memberOf}, so
 * pipelines that run as a service account resolve its roles.
 */
@RequiredArgsConstructor
public class ServiceAccountMembershipReconciler {

  /** Front50 service accounts; null when no provider is configured. */
  private final ResourceProvider<ServiceAccount> serviceAccounts;

  private final Reconciler reconciler;
  private final ReconcileLock lock;

  /** Reconcile every service account through {@code reconciler}. Returns tuples changed. */
  public long reconcileAll(Reconciler reconciler) {
    if (serviceAccounts == null) {
      return 0;
    }
    long changed = 0;
    for (var serviceAccount : serviceAccounts.getAll()) {
      changed += reconcile(reconciler, serviceAccount.getName(), serviceAccount.getMemberOf());
    }
    return changed;
  }

  /**
   * Reconcile one service account to {@code memberOf}; empty removes it. Returns tuples changed.
   */
  public long reconcile(String name, Collection<String> memberOf) {
    return reconcile(reconciler, name, memberOf);
  }

  private long reconcile(Reconciler reconciler, String name, Collection<String> memberOf) {
    var id = ZanzibarPrincipals.userId(name);
    var roles = ZanzibarPrincipals.roles(memberOf);
    var changed = new long[1];
    lock.runExclusively(
        "user:" + id, () -> changed[0] = reconciler.reconcileUserMemberships(id, roles));
    return changed[0];
  }
}
