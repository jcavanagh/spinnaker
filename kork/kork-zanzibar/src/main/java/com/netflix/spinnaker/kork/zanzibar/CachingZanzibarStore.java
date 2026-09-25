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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import com.netflix.spinnaker.security.Authorization;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * A read-through cache in front of any {@link ZanzibarStore}, for the hot decision path — {@code
 * check} (every {@code @PreAuthorize}), {@code isMember} (per-request admin resolution), and {@code
 * lookupResources} (UI list/filter). Wrap the engine adapter with this on the read/PEP path.
 *
 * <p>Entries expire after a short TTL, so revocation latency is bounded by the TTL rather than
 * immediate — the trade for cutting store load. A write through this instance evicts the caches for
 * local correctness; cross-instance writes are bounded by the TTL. Reads that reconciliation
 * depends on ({@code read}, {@code objectIds}) are never cached.
 */
public class CachingZanzibarStore implements ZanzibarStore {

  private final ZanzibarStore delegate;
  private final Cache<String, Boolean> decisions;
  private final Cache<String, Set<String>> lookups;

  public CachingZanzibarStore(ZanzibarStore delegate, Duration ttl, long maximumSize) {
    this.delegate = delegate;
    this.decisions =
        CacheBuilder.newBuilder().expireAfterWrite(ttl).maximumSize(maximumSize).build();
    this.lookups = CacheBuilder.newBuilder().expireAfterWrite(ttl).maximumSize(maximumSize).build();
  }

  @Override
  public boolean check(String userId, String type, String resourceName, Authorization action) {
    var key = "chk|" + userId + "|" + type + "|" + resourceName + "|" + action;
    var cached = decisions.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    var result = delegate.check(userId, type, resourceName, action);
    decisions.put(key, result);
    return result;
  }

  @Override
  public boolean isMember(String userId, String groupId) {
    var key = "mem|" + userId + "|" + groupId;
    var cached = decisions.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    var result = delegate.isMember(userId, groupId);
    decisions.put(key, result);
    return result;
  }

  @Override
  public Set<String> lookupResources(String userId, String type, Authorization action) {
    var key = userId + "|" + type + "|" + action;
    var cached = lookups.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    var result = delegate.lookupResources(userId, type, action);
    lookups.put(key, result);
    return result;
  }

  @Override
  public void write(Collection<ZanzibarRelationship> tuples) {
    delegate.write(tuples);
    invalidate();
  }

  @Override
  public void delete(Collection<ZanzibarRelationship> tuples) {
    delegate.delete(tuples);
    invalidate();
  }

  @Override
  public void apply(
      Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
    delegate.apply(writes, deletes);
    invalidate();
  }

  private void invalidate() {
    decisions.invalidateAll();
    lookups.invalidateAll();
  }

  // Reconciliation reads and pass-throughs must see current state — never cached.

  @Override
  public Set<ZanzibarRelationship> read(String objectType, String objectId) {
    return delegate.read(objectType, objectId);
  }

  @Override
  public Set<String> objectIds(String objectType) {
    return delegate.objectIds(objectType);
  }

  @Override
  public Set<String> groupsOf(String userId) {
    return delegate.groupsOf(userId);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void applySchema() {
    delegate.applySchema();
  }

  @Override
  public String providerId() {
    return delegate.providerId();
  }

  @Override
  public int writeBatchSize() {
    return delegate.writeBatchSize();
  }

  @Override
  public Optional<ConsistencySupport> consistency() {
    return delegate.consistency();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
