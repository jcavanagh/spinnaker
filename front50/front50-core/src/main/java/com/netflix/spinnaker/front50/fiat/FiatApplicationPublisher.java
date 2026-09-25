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

package com.netflix.spinnaker.front50.fiat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.fiat.shared.FiatResourceEvents;
import com.netflix.spinnaker.front50.model.application.ApplicationDAO;
import com.netflix.spinnaker.front50.model.application.ApplicationPermissionDAO;
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Publishes an application's current state to Fiat, as Fiat reads it from Front50. Never fails the
 * write; Fiat's bootstrap corrects a missed event.
 */
@Component
@ConditionalOnProperty("services.fiat.resource-events.enabled")
@RequiredArgsConstructor
public class FiatApplicationPublisher {

  private static final Logger log = LogManager.getLogger(FiatApplicationPublisher.class);
  private static final String APPLICATION = "application";

  private final FiatResourceEvents events;
  private final ApplicationDAO applicationDAO;
  private final Optional<ApplicationPermissionDAO> applicationPermissionDAO;
  private final ObjectMapper objectMapper;

  public void publish(String applicationName) {
    try {
      publishCurrent(applicationName);
    } catch (RuntimeException e) {
      log.error("Failed to publish application {} to Fiat", applicationName, e);
    }
  }

  private void publishCurrent(String applicationName) {
    Map<String, Object> application;
    try {
      application =
          objectMapper.convertValue(
              applicationDAO.findByName(applicationName), new TypeReference<>() {});
    } catch (NotFoundException e) {
      // As Application.getName(), which Fiat's application listing returns.
      events.deleted(APPLICATION, List.of(applicationName.toUpperCase()));
      return;
    }
    // As v2/applications?restricted=false: permissions only from a restricted permission document.
    application.remove("permissions");
    applicationPermissionDAO.ifPresent(
        dao -> {
          try {
            var permission = dao.findById(applicationName);
            if (permission.getPermissions().isRestricted()) {
              application.put("permissions", permission.getPermissions());
            }
          } catch (NotFoundException e) {
            // No permission document: unrestricted.
          }
        });
    events.changed(APPLICATION, List.of(application));
  }
}
