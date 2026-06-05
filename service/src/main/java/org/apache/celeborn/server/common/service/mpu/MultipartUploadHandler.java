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

package org.apache.celeborn.server.common.service.mpu;

import java.io.IOException;
import java.io.InputStream;

public interface MultipartUploadHandler {

  void startUpload();

  /**
   * Upload a single part.
   *
   * <p>The {@code inputStream} must be fully buffered: {@link InputStream#available()} has to
   * return the complete part length, and the stream must support {@link InputStream#mark} /
   * {@link InputStream#reset} so the underlying SDK can re-read it when a request is retried.
   * Callers therefore pass a {@link java.io.ByteArrayInputStream}. The implementation closes the
   * stream when done.
   */
  void putPart(InputStream inputStream, Integer partNumber, Boolean finalFlush) throws IOException;

  void complete();

  void abort();

  void close();
}
