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

package org.apache.celeborn;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.celeborn.S3MultipartUploadHandler.S3MultipartUploadHandlerSharedState;

public class S3MultipartUploadHandlerSuiteJ {

  @Test
  public void testGetCredentialsProviderShouldGiveDefaultProvidersOnEmptyConfig() throws Exception {
    Configuration conf = new Configuration();
    AWSCredentialProviderList providers =
        S3MultipartUploadHandler.getCredentialsProvider(null, conf);
    Assert.assertTrue(providers.size() != 0);
  }

  @Test
  public void testGetCredentialsProviderShouldReturnWebIdentityTokenCredentialsProvider()
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        Constants.AWS_CREDENTIALS_PROVIDER,
        "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider");
    AWSCredentialProviderList providers =
        S3MultipartUploadHandler.getCredentialsProvider(null, conf);
    Assert.assertEquals("WebIdentityTokenFileCredentialsProvider ", providers.listProviderNames());
  }

  @Test
  public void testSharedStateBuildsClientWithChecksumAndMd5Config() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.AWS_REGION, "us-east-1");
    conf.setBoolean(Constants.REQUEST_MD5_HEADER, true);
    conf.setBoolean(Constants.CHECKSUM_GENERATION, false);
    conf.setBoolean(Constants.CHECKSUM_VALIDATION, false);
    conf.set(Constants.ENDPOINT, "http://localhost:9878");
    conf.setBoolean(Constants.PATH_STYLE_ACCESS, true);
    RawLocalFileSystem fs = new RawLocalFileSystem();
    fs.setConf(conf);
    try (S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(fs, "test-bucket", 5, 100, 20000, 5 * 1024 * 1024)) {
      Assert.assertNotNull(state);
    }
  }

  @Test
  public void testSharedStateFallsBackToDefaultRegionForCustomEndpoint() throws Exception {
    Configuration conf = new Configuration();
    // custom endpoint, but no fs.s3a.endpoint.region configured: must still build a client.
    conf.set(Constants.ENDPOINT, "http://localhost:9878");
    conf.setBoolean(Constants.PATH_STYLE_ACCESS, true);
    RawLocalFileSystem fs = new RawLocalFileSystem();
    fs.setConf(conf);
    try (S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(fs, "test-bucket", 5, 100, 20000, 5 * 1024 * 1024)) {
      Assert.assertNotNull(state);
    }
  }

  private static ByteArrayInputStream zeros(int size) {
    return new ByteArrayInputStream(new byte[size]);
  }

  @Test
  public void testPutPartAccumulatesSmallPartsToMeetMinPartSize() throws Exception {
    int mib = 1024 * 1024;
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.when(client.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    Mockito.when(
            client.uploadPart(
                Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().eTag("etag").build());

    S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(client, "test-bucket");
    S3MultipartUploadHandler handler = new S3MultipartUploadHandler(state, "key");
    handler.startUpload();

    // Two 2 MiB non-final flushes: buffered (4 MiB < 5 MiB), nothing uploaded yet.
    handler.putPart(zeros(2 * mib), 1, false);
    handler.putPart(zeros(2 * mib), 2, false);
    Mockito.verify(client, Mockito.never())
        .uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class));

    // Third 2 MiB non-final flush: buffer reaches 6 MiB >= 5 MiB -> one 6 MiB part is uploaded.
    handler.putPart(zeros(2 * mib), 3, false);
    ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
    Mockito.verify(client, Mockito.times(1)).uploadPart(captor.capture(), Mockito.any(RequestBody.class));
    Assert.assertEquals(6L * mib, captor.getValue().contentLength().longValue());
    Assert.assertEquals(1, captor.getValue().partNumber().intValue());

    // Final flush of 1 MiB: uploaded as the last part regardless of size.
    handler.putPart(zeros(mib), 4, true);
    Mockito.verify(client, Mockito.times(2))
        .uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class));
  }

  @Test
  public void testCompleteUploadsRemainingBufferedDataWhenNoFinalFlush() throws Exception {
    int mib = 1024 * 1024;
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.when(client.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    Mockito.when(
            client.uploadPart(
                Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().eTag("etag").build());
    Mockito.when(client.listParts(Mockito.any(ListPartsRequest.class)))
        .thenReturn(
            ListPartsResponse.builder()
                .parts(Part.builder().partNumber(1).eTag("etag").build())
                .isTruncated(false)
                .build());
    Mockito.when(
            client.completeMultipartUpload(Mockito.any(CompleteMultipartUploadRequest.class)))
        .thenReturn(CompleteMultipartUploadResponse.builder().location("loc").build());

    S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(client, "test-bucket");
    S3MultipartUploadHandler handler = new S3MultipartUploadHandler(state, "key");
    handler.startUpload();

    // A single small non-final flush (2 MiB): buffered, not yet uploaded.
    handler.putPart(zeros(2 * mib), 1, false);
    Mockito.verify(client, Mockito.never())
        .uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class));

    // complete() with no finalFlush=true call must still drain and upload the buffered 2 MiB.
    handler.complete();
    ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
    Mockito.verify(client, Mockito.times(1)).uploadPart(captor.capture(), Mockito.any(RequestBody.class));
    Assert.assertEquals(2L * mib, captor.getValue().contentLength().longValue());
    Mockito.verify(client, Mockito.times(1))
        .completeMultipartUpload(Mockito.any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testPutPartUploadsLargePartDirectlyWithoutBuffering() throws Exception {
    int mib = 1024 * 1024;
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.when(client.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    Mockito.when(
            client.uploadPart(
                Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().eTag("etag").build());

    S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(client, "test-bucket");
    S3MultipartUploadHandler handler = new S3MultipartUploadHandler(state, "key");
    handler.startUpload();

    // A single 6 MiB non-final flush is already a valid part, so it is uploaded immediately
    // (fast path) rather than copied through the accumulation buffer.
    handler.putPart(zeros(6 * mib), 1, false);
    ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
    Mockito.verify(client, Mockito.times(1)).uploadPart(captor.capture(), Mockito.any(RequestBody.class));
    Assert.assertEquals(6L * mib, captor.getValue().contentLength().longValue());
  }

  @Test
  public void testPutPartRejectsOutOfOrderPartNumbers() throws Exception {
    int mib = 1024 * 1024;
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.when(client.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());

    S3MultipartUploadHandlerSharedState state =
        new S3MultipartUploadHandlerSharedState(client, "test-bucket");
    S3MultipartUploadHandler handler = new S3MultipartUploadHandler(state, "key");
    handler.startUpload();

    handler.putPart(zeros(mib), 2, false);
    // A lower (or equal) part number signals out-of-order/concurrent invocation and must fail fast.
    Assert.assertThrows(
        IllegalStateException.class, () -> handler.putPart(zeros(mib), 1, false));
  }
}
