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
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Reads a {@link RoleSource} / {@link ResourceSource} and writes the resulting relationships into
 * an {@link ZanzibarStore} in flushed batches. This is the analogue of Fiat's role/resource sync,
 * but it writes relationships rather than materializing per-user permission blobs.
 */
public class IngestionPipeline {

  private final ZanzibarStore store;
  private final int flushSize;

  public IngestionPipeline(ZanzibarStore store) {
    this(store, 1000);
  }

  public IngestionPipeline(ZanzibarStore store, int flushSize) {
    this.store = store;
    this.flushSize = flushSize;
  }

  /** Ingest the membership graph. Returns the number of relationships written. */
  public long ingestRoles(RoleSource source) {
    var buffer = new LinkedHashSet<ZanzibarRelationship>();
    var written = new long[] {0};
    source.forEach(
        (groupId, memberRef) -> {
          buffer.add(ZanzibarRelationship.member(groupId, memberRef));
          written[0] += maybeFlush(buffer);
        });
    written[0] += flush(buffer);
    return written[0];
  }

  /** Ingest resource ACLs, expanding each resource's grants. Returns the number written. */
  public long ingestResources(ResourceSource source) {
    var buffer = new LinkedHashSet<ZanzibarRelationship>();
    var written = new long[] {0};
    source.forEach(
        (type, name, grants) -> {
          buffer.addAll(Reconciler.expandResource(type, name, grants));
          written[0] += maybeFlush(buffer);
        });
    written[0] += flush(buffer);
    return written[0];
  }

  private long maybeFlush(Set<ZanzibarRelationship> buffer) {
    return buffer.size() >= flushSize ? flush(buffer) : 0;
  }

  private long flush(Set<ZanzibarRelationship> buffer) {
    if (buffer.isEmpty()) {
      return 0;
    }
    long n = buffer.size();
    store.write(buffer);
    buffer.clear();
    return n;
  }
}
