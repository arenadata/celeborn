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

package org.apache.celeborn.service.deploy.master

import java.net.URI
import java.nio.file.{Files, Path => JPath, Paths}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, timeout, verify}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.ControlMessages.ApplicationLostResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext

class MasterRemoteStorageCleanupSuite extends AnyFunSuite with MasterClusterFeature {

  private val replyTimeoutMs = 10000

  private def getTmpDir(): String = {
    val tmpDir = Files.createTempDirectory(null).toFile
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  private def newMasterConf(): CelebornConf = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, selectRandomPort().toString)
    conf
  }

  private def newMaster(conf: CelebornConf): Master = {
    val args = Array("-h", "localhost", "-p", selectRandomPort().toString)
    new Master(conf, new MasterArguments(args, conf))
  }

  private def appDir(root: String, conf: CelebornConf, appId: String): JPath =
    Paths.get(root, conf.workerWorkingDir, appId)

  private def awaitLostResponse(context: RpcCallContext): ApplicationLostResponse = {
    val captor = ArgumentCaptor.forClass(classOf[Any])
    verify(context, timeout(replyTimeoutMs)).reply(captor.capture())
    captor.getValue.asInstanceOf[ApplicationLostResponse]
  }

  test("handleApplicationLost cleans the lost app dir on every configured remote storage") {
    val hdfsRoot = getTmpDir()
    val s3Root = getTmpDir()

    val conf = newMasterConf()
    conf.set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "HDD,HDFS,S3")
    conf.set(CelebornConf.HDFS_DIR.key, s"file://$hdfsRoot")
    conf.set(CelebornConf.S3_DIR.key, s"${LocalBackedS3FileSystem.BUCKET_URI}$s3Root")
    conf.set(CelebornConf.S3_ENDPOINT_REGION.key, "us-east-1")
    conf.set("celeborn.hadoop.fs.s3a.impl", classOf[LocalBackedS3FileSystem].getName)
    conf.set("celeborn.hadoop.fs.s3a.impl.disable.cache", "true")

    val lostApp = "app-lost"
    val liveApp = "app-live"
    val roots = Seq(hdfsRoot, s3Root)
    roots.foreach { root =>
      Files.createDirectories(appDir(root, conf, lostApp))
      Files.createDirectories(appDir(root, conf, liveApp))
    }

    val probesBefore = LocalBackedS3FileSystem.probes.get()
    val master = newMaster(conf)
    val context = mock(classOf[RpcCallContext])
    try {
      master.handleApplicationLost(context, lostApp, "request-1")

      assert(awaitLostResponse(context).status === StatusCode.SUCCESS)
      // Each storage must be cleaned through the FileSystem registered for its own type: probing
      // every configured FileSystem instead fails with "Wrong FS" on the first foreign path.
      assert(LocalBackedS3FileSystem.probes.get() > probesBefore)
      roots.foreach { root =>
        assert(!Files.exists(appDir(root, conf, lostApp)), s"lost app dir survived under $root")
        assert(Files.exists(appDir(root, conf, liveApp)), s"live app dir deleted under $root")
      }
    } finally {
      master.rpcEnv.shutdown()
    }
  }

  test("handleApplicationLost replies SUCCESS when remote storage cleanup fails") {
    val s3Root = getTmpDir()

    val conf = newMasterConf()
    conf.set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "HDD,S3")
    conf.set(CelebornConf.S3_DIR.key, s"${BrokenS3FileSystem.BUCKET_URI}$s3Root")
    conf.set(CelebornConf.S3_ENDPOINT_REGION.key, "us-east-1")
    conf.set("celeborn.hadoop.fs.s3a.impl", classOf[BrokenS3FileSystem].getName)
    conf.set("celeborn.hadoop.fs.s3a.impl.disable.cache", "true")

    val lostApp = "app-lost"
    Files.createDirectories(appDir(s3Root, conf, lostApp))

    val probesBefore = BrokenS3FileSystem.probes.get()
    val master = newMaster(conf)
    val context = mock(classOf[RpcCallContext])
    try {
      master.handleApplicationLost(context, lostApp, "request-1")

      // Cleanup is best-effort: an exception escaping it would kill the handler before the reply,
      // leaving the client to block until its RPC retries are exhausted.
      assert(awaitLostResponse(context).status === StatusCode.SUCCESS)
      assert(BrokenS3FileSystem.probes.get() > probesBefore, "cleanup was never attempted")
    } finally {
      master.rpcEnv.shutdown()
    }
  }
}

object LocalBackedS3FileSystem {
  val BUCKET_URI: URI = URI.create("s3a://celeborn-cleanup-bucket")
  val probes = new AtomicInteger(0)
}

/**
 * Stands in for the S3A FileSystem: it serves s3a://celeborn-cleanup-bucket paths only and
 * rejects any other path with "Wrong FS", exactly as a real object store client does, while
 * keeping the data on local disk so a test can assert what was deleted.
 */
class LocalBackedS3FileSystem extends RawLocalFileSystem {
  override def getUri: URI = LocalBackedS3FileSystem.BUCKET_URI

  override def getScheme: String = "s3a"

  // RawLocalFileSystem answers exists() from pathToFile without consulting getFileStatus, so the
  // check that a real client performs has to be reinstated here.
  override def exists(path: Path): Boolean = {
    LocalBackedS3FileSystem.probes.incrementAndGet()
    checkPath(path)
    super.exists(path)
  }
}

object BrokenS3FileSystem {
  val BUCKET_URI: URI = URI.create("s3a://celeborn-broken-bucket")
  val probes = new AtomicInteger(0)
}

/** An S3A stand-in whose every path lookup fails, modelling an unreachable remote storage. */
class BrokenS3FileSystem extends RawLocalFileSystem {
  override def getUri: URI = BrokenS3FileSystem.BUCKET_URI

  override def getScheme: String = "s3a"

  override def exists(path: Path): Boolean = {
    BrokenS3FileSystem.probes.incrementAndGet()
    throw new IllegalArgumentException(s"Wrong FS: $path, expected: $getUri")
  }
}
