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

import java.io.IOException
import java.net.URI
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/**
 * A test FileSystem registered for the `blockfs` scheme through the
 * `celeborn.hadoop.fs.blockfs.impl` configuration property. `exists` parks the
 * calling thread until [[BlockingFileSystem.release]] is counted down, which lets
 * a test verify that the master replies to the client while the DFS cleanup is
 * still running (NGSOK-2548: reply must precede the cleanup).
 */
class BlockingFileSystem extends FileSystem {
  import BlockingFileSystem._

  override def getUri: URI = URI.create(s"blockfs://$AUTHORITY")

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    throw new IOException("open is not supported")

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throw new IOException("create is not supported")

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw new IOException("append is not supported")

  override def rename(src: Path, dst: Path): Boolean = false

  override def delete(f: Path, recursive: Boolean): Boolean = {
    offerDeleted(f)
    true
  }

  override def listStatus(f: Path): Array[FileStatus] = Array.empty

  override def setWorkingDirectory(newDir: Path): Unit = {}

  override def getWorkingDirectory: Path = new Path("/")

  override def getFileStatus(f: Path): FileStatus = new FileStatus(0, true, 1, 0, 0, f)

  override def mkdirs(f: Path, permission: FsPermission): Boolean = true

  override def exists(f: Path): Boolean = {
    ENTERED.countDown()
    RELEASE.await(BLOCK_SECONDS, TimeUnit.SECONDS)
    true
  }
}

object BlockingFileSystem {
  val AUTHORITY = "test"
  val BLOCK_SECONDS = 30L

  @volatile private var entered = new CountDownLatch(1)
  @volatile private var release = new CountDownLatch(1)
  private val deleted = new LinkedBlockingQueue[Path]()

  def ENTERED: CountDownLatch = entered
  def RELEASE: CountDownLatch = release

  def offerDeleted(p: Path): Unit = deleted.offer(p)
  def deletedEmpty: Boolean = deleted.isEmpty

  /** Returns the next deleted path, or null after the timeout. */
  def pollAny(seconds: Long): Path = deleted.poll(seconds, TimeUnit.SECONDS)

  /** Re-arms the latches and clears the recorded deletions. */
  def reset(): Unit = {
    entered = new CountDownLatch(1)
    release = new CountDownLatch(1)
    deleted.clear()
  }
}
