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

import com.google.common.math.IntMath;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.spinnaker.kork.zanzibar.consistency.ConsistencySupport;
import com.netflix.spinnaker.security.Authorization;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 * Limits a store's write requests to a {@link RateLimiter}'s rate, for bulk writes. Each write call
 * takes one permit per {@link #writeBatchSize()} relationships before it runs, so the rate holds on
 * average. Reads are not limited.
 */
@RequiredArgsConstructor
public class RateLimitedZanzibarStore implements ZanzibarStore {

  private final ZanzibarStore delegate;
  private final RateLimiter limiter;

  @Override
  public void write(Collection<ZanzibarRelationship> tuples) {
    acquire(requests(tuples));
    delegate.write(tuples);
  }

  @Override
  public void delete(Collection<ZanzibarRelationship> tuples) {
    acquire(requests(tuples));
    delegate.delete(tuples);
  }

  @Override
  public void apply(
      Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
    acquire(requests(writes) + requests(deletes));
    delegate.apply(writes, deletes);
  }

  private int requests(Collection<ZanzibarRelationship> tuples) {
    return IntMath.divide(tuples.size(), delegate.writeBatchSize(), RoundingMode.CEILING);
  }

  private void acquire(int requests) {
    if (requests > 0) {
      limiter.acquire(requests);
    }
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
  public String providerId() {
    return delegate.providerId();
  }

  @Override
  public void applySchema() {
    delegate.applySchema();
  }

  @Override
  public Set<ZanzibarRelationship> read(String objectType, String objectId) {
    return delegate.read(objectType, objectId);
  }

  @Override
  public Set<String> objectIds(String objectType) {
    return delegate.objectIds(objectType);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean check(String userId, String type, String resourceName, Authorization action) {
    return delegate.check(userId, type, resourceName, action);
  }

  @Override
  public boolean isMember(String userId, String groupId) {
    return delegate.isMember(userId, groupId);
  }

  @Override
  public Set<String> groupsOf(String userId) {
    return delegate.groupsOf(userId);
  }

  @Override
  public Set<String> lookupResources(String userId, String type, Authorization action) {
    return delegate.lookupResources(userId, type, action);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
