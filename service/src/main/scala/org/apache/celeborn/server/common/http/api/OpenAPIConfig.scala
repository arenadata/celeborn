/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.server.common.http.api

import org.glassfish.jersey.CommonProperties
import org.glassfish.jersey.server.{ResourceConfig, ServerProperties}

import org.apache.celeborn.server.common.Service

class OpenAPIConfig(serviceName: String) extends ResourceConfig {
  // Celeborn documents its REST API via OpenAPI, not WADL. Disable Jersey's WADL feature so it
  // does not look for a JAXBContext (absent since JDK 11 removed JAXB) and log a noisy
  // "JAXBContext implementation could not be found. WADL feature is disabled." warning at startup.
  property(ServerProperties.WADL_FEATURE_DISABLE, true)
  // Likewise, the JSON-only API never serves javax.activation.DataSource bodies; disabling that
  // default provider avoids the equally noisy "A class javax.activation.DataSource ... was not
  // found" warning (JAF was removed from the JDK in Java 11 too).
  property(CommonProperties.PROVIDER_DEFAULT_DISABLE, "DATASOURCE")
  packages(OpenAPIConfig.packages(serviceName): _*)
  register(classOf[CelebornOpenApiResource])
  register(classOf[CelebornScalaObjectMapper])
  register(classOf[RestExceptionMapper])
}

object OpenAPIConfig {
  val packages = Map(
    Service.MASTER -> Seq(
      "org.apache.celeborn.server.common.http.api",
      "org.apache.celeborn.service.deploy.master.http.api"),
    Service.WORKER -> Seq(
      "org.apache.celeborn.server.common.http.api",
      "org.apache.celeborn.service.deploy.worker.http.api"))
}
