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

import com.netflix.spinnaker.front50.events.ApplicationEventListener;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Publishes an application to Fiat after it is created, updated, or deleted. */
@Component
@ConditionalOnProperty("services.fiat.resource-events.enabled")
@RequiredArgsConstructor
public class ZanzibarApplicationEventListener implements ApplicationEventListener {

  private static final Set<Type> TYPES =
      Set.of(Type.POST_CREATE, Type.POST_UPDATE, Type.POST_DELETE);

  private final FiatApplicationPublisher publisher;

  @Override
  public boolean supports(Type type) {
    return TYPES.contains(type);
  }

  @Override
  public void accept(ApplicationModelEvent event) {
    publisher.publish(event.updated.getName());
  }
}
