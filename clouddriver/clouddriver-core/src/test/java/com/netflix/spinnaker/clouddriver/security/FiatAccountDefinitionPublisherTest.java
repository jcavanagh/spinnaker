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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.credentials.definition.CredentialsDefinition;
import com.netflix.spinnaker.fiat.shared.FiatResourceEvents;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class FiatAccountDefinitionPublisherTest {

  @Test
  void publishesOnlyTheAccessControlFields() {
    var events = mock(FiatResourceEvents.class);
    var publisher = new FiatAccountDefinitionPublisher(events, new ObjectMapper());

    publisher.saved(
        new Definition(
            "prod.us-west-2", Map.of("READ", List.of("eng")), List.of("legacy"), "secret-token"));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<?>> resources = ArgumentCaptor.forClass(List.class);
    verify(events).changed(eq("account"), resources.capture());
    assertThat(resources.getValue())
        .singleElement()
        .isEqualTo(
            Map.of(
                "name", "prod.us-west-2",
                "permissions", Map.of("READ", List.of("eng")),
                "requiredGroupMembership", List.of("legacy")));
  }

  @Test
  void omitsMissingAccessControlFields() {
    var events = mock(FiatResourceEvents.class);
    var publisher = new FiatAccountDefinitionPublisher(events, new ObjectMapper());

    publisher.saved(new Definition("open", null, null, null));

    verify(events).changed("account", List.of(Map.of("name", "open")));
  }

  @Test
  void aDefinitionThatCannotSerializeNeverFailsTheWrite() {
    var events = mock(FiatResourceEvents.class);
    var publisher = new FiatAccountDefinitionPublisher(events, new ObjectMapper());

    publisher.saved(new Unserializable());

    verifyNoInteractions(events);
  }

  @Test
  void deletesByName() {
    var events = mock(FiatResourceEvents.class);
    var publisher = new FiatAccountDefinitionPublisher(events, new ObjectMapper());

    publisher.deleted("prod.us-west-2");

    verify(events).deleted("account", List.of("prod.us-west-2"));
  }

  static class Definition implements CredentialsDefinition {
    private final String name;
    private final Map<String, List<String>> permissions;
    private final List<String> requiredGroupMembership;
    private final String token;

    Definition(
        String name,
        Map<String, List<String>> permissions,
        List<String> requiredGroupMembership,
        String token) {
      this.name = name;
      this.permissions = permissions;
      this.requiredGroupMembership = requiredGroupMembership;
      this.token = token;
    }

    @Override
    public String getName() {
      return name;
    }

    public Map<String, List<String>> getPermissions() {
      return permissions;
    }

    public List<String> getRequiredGroupMembership() {
      return requiredGroupMembership;
    }

    public String getToken() {
      return token;
    }
  }

  static class Unserializable implements CredentialsDefinition {
    @Override
    public String getName() {
      return "broken";
    }

    public Object getSelf() {
      return this;
    }
  }
}
