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

package com.netflix.spinnaker.kork.zanzibar;

import java.util.List;

/** Resource types the store tests use; the store accepts any type names. */
final class TestTypes {

  static final String ACCOUNT = "account";
  static final String APPLICATION = "application";
  static final String BUILD_SERVICE = "build_service";
  static final String SERVICE_ACCOUNT = "service_account";

  static final ZanzibarSchema SCHEMA =
      new ZanzibarSchema(List.of(ACCOUNT, APPLICATION, BUILD_SERVICE, SERVICE_ACCOUNT));

  private TestTypes() {}
}
