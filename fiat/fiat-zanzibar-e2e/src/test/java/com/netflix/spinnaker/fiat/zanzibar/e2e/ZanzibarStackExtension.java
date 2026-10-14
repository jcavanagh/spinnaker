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

package com.netflix.spinnaker.fiat.zanzibar.e2e;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/** Starts one {@link ZanzibarStack} for the whole run, shared by every test class. */
public class ZanzibarStackExtension implements BeforeAllCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(ZanzibarStackExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    stack(context);
  }

  @Override
  public boolean supportsParameter(ParameterContext parameter, ExtensionContext context) {
    return parameter.getParameter().getType() == ZanzibarStack.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameter, ExtensionContext context) {
    return stack(context);
  }

  private static ZanzibarStack stack(ExtensionContext context) {
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .computeIfAbsent(
            ZanzibarStack.class,
            type -> {
              try {
                return ZanzibarStack.start();
              } catch (Exception e) {
                throw new IllegalStateException("Failed to start the Zanzibar E2E stack", e);
              }
            },
            ZanzibarStack.class);
  }
}
