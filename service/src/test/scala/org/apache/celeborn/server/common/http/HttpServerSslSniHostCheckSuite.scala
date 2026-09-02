/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding ownership.
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

package org.apache.celeborn.server.common.http

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.cert.X509Certificate
import javax.net.ssl.{SNIHostName, SSLContext, SSLSocket, X509TrustManager}

import scala.collection.JavaConverters._

import org.eclipse.jetty.io.Content
import org.eclipse.jetty.server.{Handler, Request, Response, SecureRequestCustomizer}
import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.util.Callback
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.ssl.SslSampleConfigs
import org.apache.celeborn.common.util.CelebornExitKind

class HttpServerSslSniHostCheckSuite extends AnyFunSuite with BeforeAndAfterEach with Logging {

  // A DNS name only, no IP address, so that an IP-addressed request is a mismatch.
  private val certHost = "localhost"
  private val keyStorePassword = "password"

  private var keyStore: File = _
  private var tempDir: File = _
  private var httpServer: HttpServer = _

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("celeborn-http-ssl").toFile
    keyStore = new File(tempDir, "keystore.p12")
    val keyPair = SslSampleConfigs.generateKeyPair("RSA")
    val cert = SslSampleConfigs.generateCertificate(
      s"CN=$certHost",
      keyPair,
      365,
      "SHA256withRSA",
      false,
      Array(certHost),
      null,
      null)
    SslSampleConfigs.createKeyStore(
      keyStore,
      keyStorePassword,
      keyStorePassword,
      "server",
      keyPair.getPrivate,
      cert)
  }

  override def afterEach(): Unit = {
    if (httpServer != null) {
      httpServer.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      httpServer = null
    }
    if (tempDir != null) {
      Files.walk(tempDir.toPath).sorted(java.util.Comparator.reverseOrder[java.nio.file.Path]())
        .forEach(p => Files.deleteIfExists(p))
      tempDir = null
    }
  }

  test("host check rejects a Host header that the certificate does not cover") {
    val port = startServer(sniHostCheckEnabled = true)
    assert(request(port, hostHeader = s"127.0.0.1:$port")._1 === 400)
  }

  test("host check rejects a mismatching Host header even when SNI matches the certificate") {
    val port = startServer(sniHostCheckEnabled = true)
    assert(request(port, hostHeader = s"127.0.0.1:$port", sniName = Some(certHost))._1 === 400)
  }

  test("host check accepts a Host header that the certificate covers") {
    val port = startServer(sniHostCheckEnabled = true)
    assert(request(port, hostHeader = s"$certHost:$port")._1 === 200)
  }

  test("disabled host check accepts a Host header that the certificate does not cover") {
    val port = startServer(sniHostCheckEnabled = false)
    val (status, body) = request(port, hostHeader = s"127.0.0.1:$port")
    assert(status === 200)
    // The customizer must still be registered, along with the attributes it provides.
    assert(body.contains("x509=true"))
  }

  private def startServer(sniHostCheckEnabled: Boolean): Int = {
    httpServer = HttpServer(
      role = "test",
      host = "127.0.0.1",
      port = 0,
      poolSize = 8,
      stopTimeout = 0L,
      idleTimeout = 30000L,
      sslEnabled = true,
      keyStorePath = Some(keyStore.getAbsolutePath),
      keyStorePassword = Some(keyStorePassword),
      keyStoreType = Some("PKCS12"),
      keyStoreAlgorithm = None,
      sslDisallowedProtocols = Seq("SSLv2", "SSLv3"),
      sslIncludeCipherSuites = Nil,
      sslSniHostCheckEnabled = sniHostCheckEnabled)

    val handler = new Handler.Abstract {
      override def handle(request: Request, response: Response, callback: Callback): Boolean = {
        val x509 = request.getAttribute(SecureRequestCustomizer.X509_ATTRIBUTE)
        response.setStatus(200)
        Content.Sink.write(response, true, s"x509=${x509 != null}", callback)
        true
      }
    }
    val contextHandler = new ContextHandler(handler, "/")
    httpServer.rootHandler.addHandler(contextHandler)
    httpServer.start()
    if (!contextHandler.isStarted) contextHandler.start()
    httpServer.connector.getLocalPort
  }

  /** Unlike the JDK HTTP clients, a raw SSL socket allows Host and SNI to be set independently. */
  private def request(
      port: Int,
      hostHeader: String,
      sniName: Option[String] = None): (Int, String) = {
    val sslContext = SSLContext.getInstance("TLS")
    // The client side of certificate validation is not what is under test.
    sslContext.init(
      null,
      Array(new X509TrustManager {
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      }),
      null)

    val socket =
      sslContext.getSocketFactory.createSocket("127.0.0.1", port).asInstanceOf[SSLSocket]
    try {
      sniName.foreach { name =>
        val params = socket.getSSLParameters
        params.setServerNames(List[javax.net.ssl.SNIServerName](new SNIHostName(name)).asJava)
        socket.setSSLParameters(params)
      }
      socket.startHandshake()
      val out = socket.getOutputStream
      out.write(
        s"GET / HTTP/1.1\r\nHost: $hostHeader\r\nConnection: close\r\n\r\n".getBytes(
          StandardCharsets.UTF_8))
      out.flush()

      val reader =
        new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      val statusLine = reader.readLine()
      val body = new StringBuilder
      var line = reader.readLine()
      while (line != null) {
        body.append(line).append('\n')
        line = reader.readLine()
      }
      logInfo(s"Host: $hostHeader, SNI: $sniName, response: $statusLine")
      // "HTTP/1.1 400 Invalid SNI"
      (statusLine.split(" ")(1).toInt, body.toString)
    } finally {
      socket.close()
    }
  }
}
