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

package com.netflix.spinnaker.kork.zanzibar;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.security.Authorization;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Runs without Docker: per-user membership reconcile touches only that user's edges. */
class UserMembershipReconcileTest {

  @Test
  void reconcilesOnlyThatUsersEdges() {
    var store = new FakeZanzibarStore(Map.of());
    store.groupsOfSeed.put("alice", Set.of("eng", "old"));

    new Reconciler(store).reconcileUserMemberships("alice", Set.of("eng", "new"));

    assertThat(store.appliedWrites).contains(ZanzibarRelationship.member("new", "user:alice"));
    assertThat(store.appliedDeletes).contains(ZanzibarRelationship.member("old", "user:alice"));
    // "eng" was already present and stays — not re-written, not deleted.
    assertThat(store.appliedWrites)
        .doesNotContain(ZanzibarRelationship.member("eng", "user:alice"));
    assertThat(store.appliedDeletes)
        .doesNotContain(ZanzibarRelationship.member("eng", "user:alice"));
  }

  @Test
  void anUnchangedUserAppliesNothing() {
    var store = new FakeZanzibarStore(Map.of());
    store.groupsOfSeed.put("alice", Set.of("eng", "ops"));

    var changed = new Reconciler(store).reconcileUserMemberships("alice", Set.of("eng", "ops"));

    assertThat(changed).isZero();
    assertThat(store.applyCalls).isZero();
  }

  @Test
  void anUnchangedResourceAppliesNothing() {
    var store = new FakeZanzibarStore(Map.of());
    var grants = Map.of(Authorization.READ, Set.of("eng"));
    store.readSeed.put(
        "application:app1", Reconciler.expandResource("application", "app1", grants));

    var changed = new Reconciler(store).reconcileResource("application", "app1", grants);

    assertThat(changed).isZero();
    assertThat(store.applyCalls).isZero();
  }
}
