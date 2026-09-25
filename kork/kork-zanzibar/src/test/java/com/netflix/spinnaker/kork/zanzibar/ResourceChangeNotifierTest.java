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

import static com.netflix.spinnaker.kork.zanzibar.TestTypes.APPLICATION;
import static com.netflix.spinnaker.security.Authorization.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.netflix.spinnaker.kork.zanzibar.ingest.InProcessReconcileLock;
import com.netflix.spinnaker.kork.zanzibar.ingest.Reconciler;
import com.netflix.spinnaker.kork.zanzibar.ingest.ResourceChangeNotifier;
import com.netflix.spinnaker.security.Authorization;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

/** Runs without Docker: the in-process notifier reconciles the changed object through the store. */
class ResourceChangeNotifierTest {

  private static final Executor DIRECT = Runnable::run;

  private static ResourceChangeNotifier notifier(FakeZanzibarStore store) {
    return new ResourceChangeNotifier(new Reconciler(store), new InProcessReconcileLock(), DIRECT);
  }

  @Test
  void resourceChangedReconcilesToSuppliedState() {
    var store = new FakeZanzibarStore(Map.of());

    notifier(store).resourceChanged(APPLICATION, "foo", Map.of(READ, Set.of("eng")));

    assertThat(store.lastReadKey).isEqualTo("application:foo");
    assertThat(store.appliedWrites)
        .contains(ZanzibarRelationship.acl(APPLICATION, "foo", READ, "group:eng"));
  }

  @Test
  void resourceDeletedRemovesGrants() {
    var store = new FakeZanzibarStore(Map.of());
    var existing = ZanzibarRelationship.acl(APPLICATION, "foo", READ, "group:eng");
    store.readSeed.put("application:foo", Set.of(existing));

    notifier(store).resourceDeleted(APPLICATION, "foo");

    assertThat(store.appliedDeletes).contains(existing);
  }

  @Test
  void groupChangedReconcilesMembership() {
    var store = new FakeZanzibarStore(Map.of());

    notifier(store).groupChanged("eng", Set.of("user:alice"));

    assertThat(store.lastReadKey).isEqualTo("group:eng");
    assertThat(store.appliedWrites).contains(ZanzibarRelationship.member("eng", "user:alice"));
  }

  @Test
  void storeFailuresReachTheCaller() {
    var failing =
        new Reconciler(new FakeZanzibarStore(Map.of())) {
          @Override
          public long reconcileResource(
              String type, String name, Map<Authorization, Set<String>> grants) {
            throw new IllegalStateException("store down");
          }
        };
    var notifier = new ResourceChangeNotifier(failing, new InProcessReconcileLock(), DIRECT);

    assertThatThrownBy(
            () -> notifier.resourceChanged(APPLICATION, "foo", Map.of(READ, Set.of("eng"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("store down");
  }
}
