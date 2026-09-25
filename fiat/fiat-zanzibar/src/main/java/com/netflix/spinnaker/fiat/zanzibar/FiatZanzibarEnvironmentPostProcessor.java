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

import java.util.Map;
import org.apache.commons.logging.Log;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Makes {@code fiat.zanzibar.enabled=true} a one-stop switch. The store replaces the periodic role
 * sync that rebuilds every user's permissions, so this turns that sync off, ahead of any other
 * configuration. A configured value it overrides is logged. Login and service-account sync stay on;
 * they write memberships through {@link ZanzibarPermissionsRepository}.
 *
 * <p>It also defaults the Redis permissions repository off, at lowest precedence, so explicit
 * configuration wins there.
 */
public class FiatZanzibarEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final String OVERRIDES = "fiatZanzibarOverrides";
  private static final String DEFAULTS = "fiatZanzibarDefaults";

  /** UserRolesSyncer runs unless this is negative. */
  private static final Map<String, Object> SYNCS_OFF =
      Map.of("fiat.write-mode.sync-delay-timeout-ms", "-1");

  private static final Map<String, Object> DEFAULTS_OFF =
      Map.of("permissions-repository.redis.enabled", "false");

  private final Log log;

  public FiatZanzibarEnvironmentPostProcessor(DeferredLogFactory logFactory) {
    this.log = logFactory.getLog(FiatZanzibarEnvironmentPostProcessor.class);
  }

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    if (!environment.getProperty("fiat.zanzibar.enabled", Boolean.class, false)) {
      return;
    }
    var sources = environment.getPropertySources();
    if (sources.contains(OVERRIDES)) {
      return;
    }
    SYNCS_OFF.forEach(
        (name, value) -> {
          var configured = environment.getProperty(name);
          if (configured != null && !configured.equalsIgnoreCase(value.toString())) {
            log.warn(
                "Ignoring " + name + "=" + configured + ": fiat.zanzibar.enabled turns it off");
          }
        });
    sources.addFirst(new MapPropertySource(OVERRIDES, SYNCS_OFF));
    sources.addLast(new MapPropertySource(DEFAULTS, DEFAULTS_OFF));
  }
}
