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

import com.netflix.spinnaker.kork.lock.LockManager;
import com.netflix.spinnaker.kork.lock.LockManager.LockOptions;
import java.time.Duration;

/**
 * Cross-instance {@link ReconcileLock} backed by kork's distributed {@code LockManager} (Redis).
 * {@code acquireLock} runs the action only when the lock is acquired, so the bootstrap becomes a
 * true single-leader operation and per-object reconciles are serialized across instances; a
 * contended call is skipped, which the contract permits.
 */
public class LockManagerReconcileLock implements ReconcileLock {

  private final LockManager lockManager;
  private final Duration maximumDuration;

  public LockManagerReconcileLock(LockManager lockManager) {
    this(lockManager, Duration.ofMinutes(10));
  }

  public LockManagerReconcileLock(LockManager lockManager, Duration maximumDuration) {
    this.lockManager = lockManager;
    this.maximumDuration = maximumDuration;
  }

  @Override
  public void runExclusively(String key, Runnable action) {
    var options = new LockOptions().withLockName(key).withMaximumLockDuration(maximumDuration);
    lockManager.acquireLock(options, action);
  }
}
