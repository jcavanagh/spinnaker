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

package com.netflix.spinnaker.kork.zanzibar.spicedb;

import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;

/** Renders the authorization model as the SpiceDB schema DSL. */
final class SpiceDbSchema {

  private SpiceDbSchema() {}

  static String render(ZanzibarSchema schema) {
    var sb = new StringBuilder();
    sb.append("definition user {}\n\n");
    sb.append("definition group {\n\trelation member: user | group#member\n}\n");
    for (var type : schema.getResourceTypes()) {
      sb.append("\ndefinition ").append(type).append(" {\n");
      for (var action : ZanzibarSchema.actions()) {
        sb.append("\trelation ").append(action).append(": user | user:* | group#member\n");
      }
      sb.append("}\n");
    }
    return sb.toString();
  }
}
