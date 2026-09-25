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

/**
 * Serializes reconciliation work by key across the running instances. Two uses: a single global key
 * makes the bootstrap single-flight (one leader runs it), and a per-object key serializes
 * concurrent reconciles of the same resource, group, or user.
 *
 * <p>Contract is best-effort mutual exclusion: the action runs while the key's lock is held, but an
 * implementation may <em>skip</em> the action if the lock is held elsewhere. A skipped reconcile is
 * corrected only by the object's next reconcile: a later event or login, or, for resources and
 * service accounts, the next bootstrap.
 *
 * <p>{@link InProcessReconcileLock} guards within one JVM (always runs, blocking on contention) —
 * enough for a single instance and tests. {@link LockManagerReconcileLock} guards across instances
 * via kork's distributed {@code LockManager}.
 */
public interface ReconcileLock {

  /** Run {@code action} under the lock for {@code key} (may be skipped if held elsewhere). */
  void runExclusively(String key, Runnable action);
}
