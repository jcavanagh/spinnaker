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

package com.netflix.spinnaker.kork.zanzibar.openfga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.netflix.spinnaker.kork.zanzibar.ZanzibarSchema;

/**
 * Renders the authorization model as OpenFGA model JSON, for a {@code
 * WriteAuthorizationModelRequest}.
 */
final class OpenFgaModel {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private OpenFgaModel() {}

  static String json(ZanzibarSchema schema) {
    var root = MAPPER.createObjectNode();
    root.put("schema_version", "1.1");
    var types = root.putArray("type_definitions");

    types.addObject().put("type", "user");

    var group = types.addObject();
    group.put("type", "group");
    group.putObject("relations").putObject("member").putObject("this");
    var groupMeta = group.putObject("metadata").putObject("relations").putObject("member");
    var groupRefs = groupMeta.putArray("directly_related_user_types");
    groupRefs.addObject().put("type", "user");
    var groupNested = groupRefs.addObject();
    groupNested.put("type", "group");
    groupNested.put("relation", "member");

    for (var type : schema.getResourceTypes()) {
      var def = types.addObject();
      def.put("type", type);
      var relations = def.putObject("relations");
      var metaRelations = def.putObject("metadata").putObject("relations");
      for (var action : ZanzibarSchema.actions()) {
        relations.putObject(action).putObject("this");
        addResourceRelationRefs(
            metaRelations.putObject(action).putArray("directly_related_user_types"));
      }
    }
    return root.toString();
  }

  private static void addResourceRelationRefs(ArrayNode refs) {
    refs.addObject().put("type", "user");
    var wildcard = refs.addObject();
    wildcard.put("type", "user");
    wildcard.putObject("wildcard");
    var groupMembers = refs.addObject();
    groupMembers.put("type", "group");
    groupMembers.put("relation", "member");
  }
}
