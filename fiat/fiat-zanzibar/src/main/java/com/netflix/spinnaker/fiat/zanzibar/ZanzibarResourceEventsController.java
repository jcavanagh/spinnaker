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

package com.netflix.spinnaker.fiat.zanzibar;

import com.netflix.spinnaker.fiat.model.resources.ResourceType;
import com.netflix.spinnaker.kork.zanzibar.ingest.ResourceChangeNotifier;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Where sources publish resource changes, in Fiat's resource shape. Internal only, in the same
 * trust boundary as {@code /roles}; Gate never proxies it.
 */
@RestController
@RequestMapping("/zanzibar")
@ConditionalOnProperty(prefix = "fiat.zanzibar", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class ZanzibarResourceEventsController {

  private final ZanzibarResourcePermissions permissions;
  private final ResourceChangeNotifier notifier;
  private final ServiceAccountMembershipReconciler serviceAccounts;
  private final ZanzibarBootstrap bootstrap;

  @PostMapping("/resources/{resourceType}")
  public void changed(
      @PathVariable String resourceType, @RequestBody List<Map<String, Object>> resources) {
    var type = ResourceType.parse(resourceType);
    for (var resource : resources) {
      if (ResourceType.SERVICE_ACCOUNT.equals(type)) {
        var serviceAccount = permissions.serviceAccount(resource);
        serviceAccounts.reconcile(serviceAccount.getName(), serviceAccount.getMemberOf());
        continue;
      }
      var name = String.valueOf(resource.get("name"));
      permissions
          .effective(type, resource)
          .ifPresentOrElse(
              effective ->
                  notifier.resourceChanged(type.getName(), name, ZanzibarTypes.grants(effective)),
              () -> notifier.resourceDeleted(type.getName(), name));
    }
  }

  @PostMapping("/resources/{resourceType}/delete")
  public void deleted(@PathVariable String resourceType, @RequestBody List<String> names) {
    var type = ResourceType.parse(resourceType);
    for (var name : names) {
      if (ResourceType.SERVICE_ACCOUNT.equals(type)) {
        serviceAccounts.reconcile(name, List.of());
      } else {
        notifier.resourceDeleted(type.getName(), name);
      }
    }
  }

  /**
   * Runs the bootstrap, writing at most {@code rate} requests per second; 409 if one is running.
   */
  @PostMapping("/bootstrap")
  public ResponseEntity<Map<String, Long>> bootstrap(
      @RequestParam(defaultValue = "1000") int rate) {
    if (rate <= 0) {
      throw new IllegalArgumentException("rate must be positive");
    }
    return bootstrap
        .run(rate)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.status(HttpStatus.CONFLICT).build());
  }
}
