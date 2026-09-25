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

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.Lock;

/**
 * In-JVM {@link ReconcileLock} using bounded striped locks. Guards within a single instance only.
 */
public class InProcessReconcileLock implements ReconcileLock {

  private final Striped<Lock> striped;

  public InProcessReconcileLock() {
    this(256);
  }

  public InProcessReconcileLock(int stripes) {
    this.striped = Striped.lock(stripes);
  }

  @Override
  public void runExclusively(String key, Runnable action) {
    var lock = striped.get(key);
    lock.lock();
    try {
      action.run();
    } finally {
      lock.unlock();
    }
  }
}
