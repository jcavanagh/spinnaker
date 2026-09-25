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

import com.netflix.spinnaker.security.Authorization;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;

/**
 * The in-process change hook a source service calls right after it mutates a resource or group. It
 * reconciles just that object into the store — passing the new state the caller already holds, so
 * no re-read is needed — on an executor (the caller's write path isn't blocked) and under a
 * per-object {@link ReconcileLock} (so concurrent changes to one object serialize). Reconcile is
 * idempotent; with a same-thread executor, failures propagate to the caller, which retries.
 *
 * <p>No message bus: the caller pushes directly. A remote transport (webhook / pub-sub) can be
 * added later behind the same reconcile core for services that shouldn't depend on the store.
 */
@RequiredArgsConstructor
public class ResourceChangeNotifier {

  private final Reconciler reconciler;
  private final ReconcileLock lock;
  private final Executor executor;

  /** A resource was created or its permissions changed; reconcile to the supplied state. */
  public void resourceChanged(String type, String name, Map<Authorization, Set<String>> grants) {
    submit(type + ":" + name, () -> reconciler.reconcileResource(type, name, grants));
  }

  /** A resource was deleted; remove all of its grants. */
  public void resourceDeleted(String type, String name) {
    submit(type + ":" + name, () -> reconciler.deleteResource(type, name));
  }

  /** A group's membership changed; reconcile to the supplied members. */
  public void groupChanged(String groupId, Set<String> members) {
    submit("group:" + groupId, () -> reconciler.reconcileGroup(groupId, members));
  }

  private void submit(String key, Runnable action) {
    executor.execute(() -> lock.runExclusively(key, action));
  }
}
