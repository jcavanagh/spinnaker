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

package com.netflix.spinnaker.fiat.shared;

import com.netflix.spinnaker.kork.core.RetrySupport;
import com.netflix.spinnaker.kork.retrofit.Retrofit2SyncCall;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Publishes resource changes to Fiat's store in the background, in order. Retries, and never throws
 * into the caller.
 */
@Component
@ConditionalOnProperty("services.fiat.resource-events.enabled")
@RequiredArgsConstructor
public class FiatResourceEvents implements DisposableBean {

  private static final Logger log = LogManager.getLogger(FiatResourceEvents.class);

  private final FiatService fiatService;
  private final RetrySupport retrySupport = new RetrySupport();

  /** A single thread keeps this instance's events in order. */
  private final ExecutorService publisher =
      Executors.newSingleThreadExecutor(
          r -> {
            var thread = new Thread(r, "fiat-resource-events");
            thread.setDaemon(true);
            return thread;
          });

  /** Publish changed resources, e.g. {@code resourceType} {@code application}. */
  public void changed(String resourceType, List<?> resources) {
    publish(
        resourceType,
        resources.size(),
        () -> Retrofit2SyncCall.execute(fiatService.resourcesChanged(resourceType, resources)));
  }

  public void deleted(String resourceType, List<String> names) {
    publish(
        resourceType,
        names.size(),
        () -> Retrofit2SyncCall.execute(fiatService.resourcesDeleted(resourceType, names)));
  }

  @Override
  public void destroy() throws InterruptedException {
    publisher.shutdown();
    publisher.awaitTermination(10, TimeUnit.SECONDS);
  }

  private void publish(String resourceType, int count, Runnable call) {
    if (count == 0) {
      return;
    }
    publisher.execute(
        () -> {
          try {
            retrySupport.retry(
                () -> {
                  call.run();
                  return null;
                },
                3,
                Duration.ofSeconds(1),
                true);
          } catch (RuntimeException e) {
            log.error("Failed to publish {} {} change(s) to Fiat", count, resourceType, e);
          }
        });
  }
}
