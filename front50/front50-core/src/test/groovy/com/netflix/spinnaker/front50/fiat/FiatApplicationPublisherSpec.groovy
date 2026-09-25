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

package com.netflix.spinnaker.front50.fiat

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.fiat.model.Authorization
import com.netflix.spinnaker.fiat.model.resources.Permissions
import com.netflix.spinnaker.fiat.shared.FiatResourceEvents
import com.netflix.spinnaker.front50.model.application.Application
import com.netflix.spinnaker.front50.model.application.ApplicationDAO
import com.netflix.spinnaker.front50.model.application.ApplicationPermissionDAO
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException
import spock.lang.Specification

class FiatApplicationPublisherSpec extends Specification {

  FiatResourceEvents events = Mock()
  ApplicationDAO applicationDAO = Mock()
  ApplicationPermissionDAO permissionDAO = Mock()

  def publisher = new FiatApplicationPublisher(
    events, applicationDAO, Optional.of(permissionDAO), new ObjectMapper())

  def "publishes the application with its restricted permissions"() {
    given:
    applicationDAO.findByName("app1") >> application("app1")
    permissionDAO.findById("app1") >> permission("app1", new Permissions.Builder().add(Authorization.READ, "eng").build())

    when:
    publisher.publish("app1")

    then:
    1 * events.changed("application", { List resources ->
      resources.size() == 1 &&
        resources[0].name == "APP1" &&
        resources[0].email == "owner@example.com" &&
        (resources[0].permissions as Permissions).get(Authorization.READ) == ["eng"] as Set
    })
    0 * events.deleted(_, _)
  }

  def "publishes the application without permissions when its document is unrestricted"() {
    given:
    applicationDAO.findByName("app1") >> application("app1")
    permissionDAO.findById("app1") >> permission("app1", Permissions.EMPTY)

    when:
    publisher.publish("app1")

    then:
    1 * events.changed("application", { List resources ->
      resources[0].name == "APP1" && !resources[0].containsKey("permissions")
    })
  }

  def "publishes the application without permissions when it has no document"() {
    given:
    applicationDAO.findByName("app1") >> application("app1")
    permissionDAO.findById("app1") >> { throw new NotFoundException("none") }

    when:
    publisher.publish("app1")

    then:
    1 * events.changed("application", { List resources ->
      resources[0].name == "APP1" && !resources[0].containsKey("permissions")
    })
  }

  def "publishes a delete, by the uppercase name, when the application is gone"() {
    given:
    applicationDAO.findByName("app1") >> { throw new NotFoundException("gone") }

    when:
    publisher.publish("app1")

    then:
    1 * events.deleted("application", ["APP1"])
    0 * events.changed(_, _)
  }

  def "a failed read never fails the write"() {
    given:
    applicationDAO.findByName("app1") >> { throw new IllegalStateException("storage down") }

    when:
    publisher.publish("app1")

    then:
    noExceptionThrown()
    0 * events._
  }

  def "the listeners publish the changed application by name"() {
    given:
    def publisher = Mock(FiatApplicationPublisher)
    def applicationListener = new ZanzibarApplicationEventListener(publisher)
    def permissionListener = new ZanzibarApplicationPermissionEventListener(publisher)
    def app = application("app1")

    when:
    applicationListener.accept(
      new com.netflix.spinnaker.front50.events.ApplicationEventListener.ApplicationModelEvent(
        com.netflix.spinnaker.front50.events.ApplicationEventListener.Type.POST_DELETE, app, app))
    def returned = permissionListener.call(null, permission("app2", Permissions.EMPTY))

    then:
    1 * publisher.publish("APP1")
    1 * publisher.publish("app2")
    returned.name == "app2"
  }

  private static Application application(String name) {
    def application = new Application()
    application.name = name
    application.set("email", "owner@example.com")
    return application
  }

  private static Application.Permission permission(String name, Permissions permissions) {
    def permission = new Application.Permission()
    permission.name = name
    permission.permissions = permissions
    return permission
  }
}
