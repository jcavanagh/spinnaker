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

import com.google.common.util.concurrent.RateLimiter;
import com.netflix.spinnaker.fiat.zanzibar.resource.ZanzibarResourceType;
import com.netflix.spinnaker.kork.zanzibar.RateLimitedZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.kork.zanzibar.ingest.ReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Reconciles the whole store from its sources: every resource ACL, then every service account's
 * memberships. Runs only when an admin calls {@code POST /zanzibar/bootstrap}; change events keep
 * the store current otherwise.
 *
 * <p>The resource pass runs under one global {@link ReconcileLock} key, so across many instances
 * one runs at a time.
 */
@RequiredArgsConstructor
public class ZanzibarBootstrap {

  static final String SWEEP_LOCK_KEY = "zanzibar-full-sweep";

  private static final Logger log = LogManager.getLogger(ZanzibarBootstrap.class);

  private final ZanzibarStore store;
  private final ReconcileLock lock;
  private final List<ZanzibarResourceType> types;
  private final ServiceAccountMembershipReconciler serviceAccounts;
  private final AtomicBoolean running = new AtomicBoolean();

  /**
   * Runs the bootstrap, writing at most {@code rate} requests per second. Returns the tuples
   * changed and the duration, or empty when a bootstrap is already running on this instance.
   */
  public Optional<Map<String, Long>> run(int rate) {
    var limiter = RateLimiter.create(rate);
    if (!running.compareAndSet(false, true)) {
      return Optional.empty();
    }
    try {
      var start = System.nanoTime();
      var reconciler = new Reconciler(new RateLimitedZanzibarStore(store, limiter));
      var resources = new long[1];
      lock.runExclusively(SWEEP_LOCK_KEY, () -> resources[0] = reconcileResources(reconciler));
      var accounts = serviceAccounts.reconcileAll(reconciler);
      log.info("Zanzibar bootstrap reconciled service accounts ({} tuples changed)", accounts);
      return Optional.of(
          Map.of(
              "resourceTuplesChanged",
              resources[0],
              "serviceAccountTuplesChanged",
              accounts,
              "durationMs",
              (System.nanoTime() - start) / 1_000_000));
    } finally {
      running.set(false);
    }
  }

  /** Reconciles every listed resource, then deletes what the store holds but nothing lists. */
  private long reconcileResources(Reconciler reconciler) {
    var start = System.nanoTime();
    long changed = 0;
    for (var type : types) {
      var resources = type.all();
      if (resources.isEmpty()) {
        // Without a provider nothing lists the type, so its stored ACLs are left alone.
        continue;
      }
      var name = type.type().getName();
      var present = new HashSet<String>();
      for (var resource : resources.get()) {
        changed +=
            reconciler.reconcileResource(
                name, resource.getName(), ZanzibarTypes.grants(resource.getPermissions()));
        present.add(resource.getName());
      }
      for (var id : store.objectIds(name)) {
        if (!present.contains(id)) {
          changed += reconciler.deleteResource(name, id);
        }
      }
    }
    log.info(
        "[{}] full sweep changed {} resource tuples in {} ms",
        store.providerId(),
        changed,
        (System.nanoTime() - start) / 1_000_000);
    return changed;
  }
}
