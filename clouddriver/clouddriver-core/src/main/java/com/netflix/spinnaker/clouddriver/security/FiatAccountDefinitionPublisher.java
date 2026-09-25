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

package com.netflix.spinnaker.clouddriver.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.credentials.definition.CredentialsDefinition;
import com.netflix.spinnaker.fiat.shared.FiatResourceEvents;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.util.ClassUtils;

/**
 * Publishes an account's ACL to Fiat when its definition is written, in the fields Fiat reads from
 * {@code /credentials}. Never fails the write; Fiat's bootstrap corrects a missed event.
 */
@Log4j2
@RequiredArgsConstructor
public class FiatAccountDefinitionPublisher {

  private static final String ACCOUNT = "account";

  /** Fiat authorizes cloud accounts only. */
  private static final String ARTIFACT_ACCOUNT =
      "com.netflix.spinnaker.clouddriver.artifacts.config.ArtifactAccount";

  private final FiatResourceEvents events;
  private final ObjectMapper objectMapper;

  public void saved(CredentialsDefinition definition) {
    if (isArtifactAccount(definition)) {
      return;
    }
    Map<String, Object> json;
    try {
      json = objectMapper.convertValue(definition, new TypeReference<>() {});
    } catch (RuntimeException e) {
      log.error("Failed to publish account {} to Fiat", definition.getName(), e);
      return;
    }
    var account = new LinkedHashMap<String, Object>();
    account.put("name", definition.getName());
    for (var field : List.of("permissions", "requiredGroupMembership")) {
      if (json.get(field) != null) {
        account.put(field, json.get(field));
      }
    }
    events.changed(ACCOUNT, List.of(account));
  }

  public void deleted(String accountName) {
    events.deleted(ACCOUNT, List.of(accountName));
  }

  private static boolean isArtifactAccount(CredentialsDefinition definition) {
    return ClassUtils.getAllInterfacesForClassAsSet(definition.getClass()).stream()
        .anyMatch(type -> ARTIFACT_ACCOUNT.equals(type.getName()));
  }
}
