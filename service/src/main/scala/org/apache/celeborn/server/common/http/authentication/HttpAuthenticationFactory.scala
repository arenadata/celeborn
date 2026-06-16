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

package org.apache.celeborn.server.common.http.authentication

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.reflect.DynConstructors
import org.apache.celeborn.spi.authentication.{PasswdAuthenticationProvider, TokenAuthenticationProvider}

object HttpAuthenticationFactory {
  def getPasswordAuthenticationProvider(
      providerClass: String,
      conf: CelebornConf): PasswdAuthenticationProvider = {
    createAuthenticationProvider(providerClass, classOf[PasswdAuthenticationProvider], conf)
  }

  def getTokenAuthenticationProvider(
      providerClass: String,
      conf: CelebornConf): TokenAuthenticationProvider = {
    createAuthenticationProvider(providerClass, classOf[TokenAuthenticationProvider], conf)
  }

  private def createAuthenticationProvider[T](
      className: String,
      expected: Class[T],
      conf: CelebornConf): T = {
    try {
      DynConstructors.builder(expected)
        .impl(className, classOf[CelebornConf])
        .impl(className)
        .buildChecked[T]()
        .newInstance(conf)
    } catch {
      case e: Exception =>
        throw new CelebornException(
          s"$className must extend of ${expected.getName}",
          e)
    }
  }
}
