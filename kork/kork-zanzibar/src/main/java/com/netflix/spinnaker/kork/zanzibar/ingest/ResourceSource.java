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

/**
 * Streams resources and their embedded ACLs into the ingestion pipeline — the resource side of
 * authorization. An implementation adapts a resource authority (Front50 applications/service
 * accounts, Clouddriver accounts, Igor build services, Kayenta metrics accounts) or a generator. A
 * resource whose grants name no roles is world-accessible.
 */
@FunctionalInterface
public interface ResourceSource {

  void forEach(ResourceConsumer consumer);

  @FunctionalInterface
  interface ResourceConsumer {
    void accept(String type, String name, Map<Authorization, Set<String>> grants);
  }
}
