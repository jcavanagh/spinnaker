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

import com.netflix.spinnaker.security.Authorization;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Benign {@link ZanzibarStore} for tests. Answers {@link #isMember} for admin resolution, records
 * {@link #apply} inputs, and can seed {@link #read} results so reconcile behavior is observable.
 */
final class FakeZanzibarStore implements ZanzibarStore {

  private final Map<String, Set<String>> membersByGroup;

  int checkCalls;
  int isMemberCalls;
  int lookupCalls;
  int applyCalls;

  final List<ZanzibarRelationship> appliedWrites = new ArrayList<>();
  final List<ZanzibarRelationship> appliedDeletes = new ArrayList<>();
  final Map<String, Set<ZanzibarRelationship>> readSeed = new HashMap<>();
  final Map<String, Set<String>> groupsOfSeed = new HashMap<>();
  String lastReadKey;

  FakeZanzibarStore(Map<String, Set<String>> membersByGroup) {
    this.membersByGroup = membersByGroup;
  }

  @Override
  public boolean isMember(String userId, String groupId) {
    isMemberCalls++;
    return membersByGroup.getOrDefault(groupId, Set.of()).contains(userId);
  }

  @Override
  public String providerId() {
    return "fake";
  }

  @Override
  public boolean isEmpty() {
    return membersByGroup.isEmpty() && readSeed.isEmpty();
  }

  @Override
  public void applySchema() {}

  @Override
  public void write(Collection<ZanzibarRelationship> tuples) {
    appliedWrites.addAll(tuples);
  }

  @Override
  public void delete(Collection<ZanzibarRelationship> tuples) {
    appliedDeletes.addAll(tuples);
  }

  @Override
  public Set<ZanzibarRelationship> read(String objectType, String objectId) {
    lastReadKey = objectType + ":" + objectId;
    return new LinkedHashSet<>(readSeed.getOrDefault(lastReadKey, Set.of()));
  }

  @Override
  public Set<String> objectIds(String objectType) {
    return new LinkedHashSet<>();
  }

  @Override
  public Set<String> groupsOf(String userId) {
    return new LinkedHashSet<>(groupsOfSeed.getOrDefault(userId, Set.of()));
  }

  @Override
  public void apply(
      Collection<ZanzibarRelationship> writes, Collection<ZanzibarRelationship> deletes) {
    applyCalls++;
    appliedWrites.addAll(writes);
    appliedDeletes.addAll(deletes);
  }

  @Override
  public boolean check(String userId, String type, String resourceName, Authorization action) {
    checkCalls++;
    return false;
  }

  @Override
  public Set<String> lookupResources(String userId, String type, Authorization action) {
    lookupCalls++;
    return new LinkedHashSet<>();
  }

  @Override
  public void close() {}
}
