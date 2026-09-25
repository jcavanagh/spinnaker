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

package com.netflix.spinnaker.fiat.zanzibar.resource;

import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.fiat.model.resources.Resource;
import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** A resource type the store holds ACLs for. */
public interface ZanzibarResourceType {

  ResourceType type();

  /** Fiat's model for a resource the store says the user reaches. */
  Resource newResource(String name, Permissions permissions);

  /** Effective permissions for a published resource, or empty when Fiat would omit it. */
  Optional<Permissions> effective(Map<String, Object> resource);

  /**
   * Every resource of this type from Fiat's provider; {@code Optional.empty()} without a provider.
   */
  Optional<Collection<? extends Resource.AccessControlled>> all();
}
