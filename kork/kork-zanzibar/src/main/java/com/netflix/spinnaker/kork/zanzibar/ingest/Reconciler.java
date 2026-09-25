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

package com.netflix.spinnaker.kork.zanzibar.ingest;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarRelationship;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarStore;
import com.netflix.spinnaker.security.Authorization;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 * Reconciles an object's relationships to a desired state — the update/delete path a durable store
 * requires (unlike Fiat, which recomputes a per-user blob each sync). It reads the object's current
 * tuples, diffs against the desired set, and applies the adds and removes atomically, so a
 * permission change never leaves a stale grant behind and a deletion never orphans tuples.
 */
@RequiredArgsConstructor
public class Reconciler {

  private final ZanzibarStore store;

  /**
   * Bring a resource's ACLs in line with its current grants (action to roles; no roles at all means
   * unrestricted). Returns tuples changed.
   */
  public long reconcileResource(String type, String name, Map<Authorization, Set<String>> grants) {
    return reconcile(type, name, expandResource(type, name, grants));
  }

  /** Remove every ACL for a resource (it was deleted upstream). Returns tuples removed. */
  public long deleteResource(String type, String name) {
    return reconcile(type, name, Set.of());
  }

  /**
   * Bring a group's membership in line with {@code memberRefs} ({@code "user:x"}/{@code
   * "group:y"}).
   */
  public long reconcileGroup(String groupId, Set<String> memberRefs) {
    var desired = new LinkedHashSet<ZanzibarRelationship>();
    for (var member : memberRefs) {
      desired.add(ZanzibarRelationship.member(groupId, member));
    }
    return reconcile("group", groupId, desired);
  }

  /**
   * Bring exactly {@code userId}'s memberships in line with {@code roleNames} — adds/removes only
   * that user's edges (never other members'), so it's safe with a per-user directory provider that
   * can't enumerate a group's full membership.
   */
  public long reconcileUserMemberships(String userId, Set<String> roleNames) {
    var subject = "user:" + userId;
    var desired = new LinkedHashSet<ZanzibarRelationship>();
    for (var role : roleNames) {
      desired.add(ZanzibarRelationship.member(role, subject));
    }
    var current = new LinkedHashSet<ZanzibarRelationship>();
    for (var group : store.groupsOf(userId)) {
      current.add(ZanzibarRelationship.member(group, subject));
    }
    var adds = new LinkedHashSet<>(desired);
    adds.removeAll(current);
    var removes = new LinkedHashSet<>(current);
    removes.removeAll(desired);
    if (!adds.isEmpty() || !removes.isEmpty()) {
      store.apply(adds, removes);
    }
    return (long) adds.size() + removes.size();
  }

  private long reconcile(String objectType, String objectId, Set<ZanzibarRelationship> desired) {
    var current = store.read(objectType, objectId);
    var adds = new LinkedHashSet<>(desired);
    adds.removeAll(current);
    var removes = new LinkedHashSet<>(current);
    removes.removeAll(desired);
    if (!adds.isEmpty() || !removes.isEmpty()) {
      store.apply(adds, removes);
    }
    return (long) adds.size() + removes.size();
  }

  /**
   * Expand a resource's grants into ACL tuples (shared with {@link IngestionPipeline}). A resource
   * whose grants name no roles is unrestricted.
   */
  public static Set<ZanzibarRelationship> expandResource(
      String type, String name, Map<Authorization, Set<String>> grants) {
    var tuples = new LinkedHashSet<ZanzibarRelationship>();
    if (grants.values().stream().anyMatch(roles -> !roles.isEmpty())) {
      grants.forEach(
          (action, roles) ->
              roles.forEach(
                  role ->
                      tuples.add(ZanzibarRelationship.acl(type, name, action, "group:" + role))));
    } else {
      // Non-restricted resources are world-accessible for every action (Fiat semantics).
      for (var action : Authorization.values()) {
        tuples.add(ZanzibarRelationship.acl(type, name, action, "user:*"));
      }
    }
    return tuples;
  }
}
