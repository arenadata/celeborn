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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class S3MultipartUploadHandler implements MultipartUploadHandler {

  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploadHandler.class);

  private String uploadId;
  private final String key;

  private final S3MultipartUploadHandlerSharedState sharedState;

  public static class S3MultipartUploadHandlerSharedState implements AutoCloseable {

    private final S3Client s3Client;
    private final String bucketName;
    private final int s3MultiplePartUploadMaxRetries;
    private final int baseDelay;
    private final int maxBackoff;

    public S3MultipartUploadHandlerSharedState(
        FileSystem hadoopFs,
        String bucketName,
        Integer s3MultiplePartUploadMaxRetries,
        Integer baseDelay,
        Integer maxBackoff)
        throws IOException, URISyntaxException {
      this.bucketName = bucketName;
      this.s3MultiplePartUploadMaxRetries = s3MultiplePartUploadMaxRetries;
      this.baseDelay = baseDelay;
      this.maxBackoff = maxBackoff;
      Configuration conf = hadoopFs.getConf();
      URI binding = new URI(String.format("s3a://%s", bucketName));

      BackoffStrategy backoffStrategy =
          BackoffStrategy.exponentialDelay(
              Duration.ofMillis(baseDelay), Duration.ofMillis(maxBackoff));
      S3ClientBuilder builder =
          S3Client.builder()
              .credentialsProvider(getCredentialsProvider(binding, conf))
              .overrideConfiguration(
                  override -> {
                    override.retryStrategy(
                        AwsRetryStrategy.standardRetryStrategy()
                            .toBuilder()
                            // maxAttempts counts total tries, so it is retries + 1 to honour the
                            // "max retries" config semantics (default 5 retries -> 6 attempts).
                            .maxAttempts(Math.max(1, s3MultiplePartUploadMaxRetries + 1))
                            .backoffStrategy(backoffStrategy)
                            .throttlingBackoffStrategy(backoffStrategy)
                            .build());
                    // Only hook the request path with the progress logging interceptor when
                    // debug logging is actually enabled, to keep the hot path untouched otherwise.
                    if (logger.isDebugEnabled()) {
                      override.addExecutionInterceptor(new CompleteUploadProgressLogger());
                    }
                  });

      // HADOOP-19654: keep compatibility with third party S3 stores such as Ozone s3g.
      // Newer AWS SDKs add a CRC32 flexible checksum to every request and stop sending
      // Content-MD5, which third party stores reject. Mirror the switches that the
      // hadoop-aws DefaultS3ClientFactory applies, reading the same Configuration keys.
      if (conf.getBoolean(Constants.REQUEST_MD5_HEADER, Constants.DEFAULT_REQUEST_MD5_HEADER)) {
        logger.debug("MD5 header enabled for bucket {}", bucketName);
        builder.addPlugin(LegacyMd5Plugin.create());
      }
      RequestChecksumCalculation checksumCalculation =
          conf.getBoolean(Constants.CHECKSUM_GENERATION, Constants.DEFAULT_CHECKSUM_GENERATION)
              ? RequestChecksumCalculation.WHEN_SUPPORTED
              : RequestChecksumCalculation.WHEN_REQUIRED;
      builder.requestChecksumCalculation(checksumCalculation);
      ResponseChecksumValidation checksumValidation =
          conf.getBoolean(Constants.CHECKSUM_VALIDATION, Constants.CHECKSUM_VALIDATION_DEFAULT)
              ? ResponseChecksumValidation.WHEN_SUPPORTED
              : ResponseChecksumValidation.WHEN_REQUIRED;
      builder.responseChecksumValidation(checksumValidation);

      String region = conf.get(Constants.AWS_REGION);
      // for MinIO / Ozone s3g and other custom endpoints
      String endpoint = conf.get(Constants.ENDPOINT);
      boolean customEndpoint = !StringUtils.isEmpty(endpoint);
      if (customEndpoint) {
        builder
            .endpointOverride(getS3Endpoint(endpoint, conf))
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(conf.getBoolean(Constants.PATH_STYLE_ACCESS, false))
                    .build());
      }
      if (!StringUtils.isEmpty(region)) {
        builder.region(Region.of(region));
      } else if (customEndpoint) {
        // SDK v2 requires a region even with a custom endpoint (for SigV4 signing).
        // S3-compatible stores (MinIO / Ozone s3g) ignore it, so fall back to a placeholder.
        builder.region(Region.US_EAST_1);
      }
      this.s3Client = builder.build();
    }

    @Override
    public void close() {
      if (s3Client != null) {
        s3Client.close();
      }
    }
  }

  public S3MultipartUploadHandler(AutoCloseable sharedState, String key) {
    this.sharedState = (S3MultipartUploadHandlerSharedState) sharedState;
    this.key = key;
  }

  @Override
  public void startUpload() {
    CreateMultipartUploadRequest initRequest =
        CreateMultipartUploadRequest.builder().bucket(sharedState.bucketName).key(key).build();
    CreateMultipartUploadResponse initResponse =
        sharedState.s3Client.createMultipartUpload(initRequest);
    this.uploadId = initResponse.uploadId();
  }

  @Override
  public void putPart(InputStream inputStream, Integer partNumber, Boolean finalFlush)
      throws IOException {
    try (InputStream inStream = inputStream) {
      int partSize = inStream.available();
      if (partSize == 0) {
        logger.debug(
            "key {} uploadId {} part size is 0 for part number {} finalFlush {}",
            key,
            uploadId,
            partNumber,
            finalFlush);
        return;
      }
      UploadPartRequest uploadRequest =
          UploadPartRequest.builder()
              .bucket(sharedState.bucketName)
              .key(key)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentLength((long) partSize)
              .build();
      sharedState.s3Client.uploadPart(
          uploadRequest, RequestBody.fromInputStream(inStream, partSize));
      logger.debug(
          "key {} uploadId {} part number {} uploaded with size {} finalFlush {}",
          key,
          uploadId,
          partNumber,
          partSize,
          finalFlush);
    } catch (RuntimeException | IOException e) {
      logger.error("Failed to upload part", e);
      throw e;
    }
  }

  @Override
  public void complete() {
    List<CompletedPart> completedParts = new ArrayList<>();
    ListPartsRequest.Builder listPartsRequestBuilder =
        ListPartsRequest.builder().bucket(sharedState.bucketName).key(key).uploadId(uploadId);
    ListPartsResponse partListing;
    do {
      partListing = sharedState.s3Client.listParts(listPartsRequestBuilder.build());
      for (Part part : partListing.parts()) {
        completedParts.add(
            CompletedPart.builder().partNumber(part.partNumber()).eTag(part.eTag()).build());
      }
      listPartsRequestBuilder.partNumberMarker(partListing.nextPartNumberMarker());
    } while (Boolean.TRUE.equals(partListing.isTruncated()));
    if (completedParts.isEmpty()) {
      logger.debug(
          "bucket {} key {} uploadId {} has no parts uploaded, aborting upload",
          sharedState.bucketName,
          key,
          uploadId);
      abort();
      logger.debug(
          "bucket {} key {} upload completed with size {}", sharedState.bucketName, key, 0);
      return;
    }
    CompleteMultipartUploadRequest compRequest =
        CompleteMultipartUploadRequest.builder()
            .bucket(sharedState.bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build();
    // Retries (bounded by maxAttempts, with exponential backoff between baseDelay and maxBackoff)
    // are handled by the shared client's retry strategy, so this is a single call: no nested
    // manual retry loop that would otherwise multiply the attempt count.
    CompleteMultipartUploadResponse compResult =
        sharedState.s3Client.completeMultipartUpload(compRequest);
    logger.debug(
        "bucket {} key {} uploadId {} upload completed location is in {} ",
        sharedState.bucketName,
        key,
        uploadId,
        compResult.location());
  }

  @Override
  public void abort() {
    AbortMultipartUploadRequest abortMultipartUploadRequest =
        AbortMultipartUploadRequest.builder()
            .bucket(sharedState.bucketName)
            .key(key)
            .uploadId(uploadId)
            .build();
    sharedState.s3Client.abortMultipartUpload(abortMultipartUploadRequest);
  }

  @Override
  public void close() {}

  static AWSCredentialProviderList getCredentialsProvider(URI binding, Configuration conf)
      throws IOException {
    return CredentialProviderListFactory.createAWSCredentialProviderList(binding, conf);
  }

  /**
   * Replacement for the SDK v1 {@code ProgressListener} that was attached to the complete multipart
   * upload request. SDK v2 has no request level progress listener, so this client level {@link
   * ExecutionInterceptor} reproduces the same debug logging, scoped to the {@code
   * CompleteMultipartUpload} call by inspecting the request type.
   */
  static class CompleteUploadProgressLogger implements ExecutionInterceptor {

    @Override
    public void beforeExecution(
        Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
      if (context.request() instanceof CompleteMultipartUploadRequest) {
        CompleteMultipartUploadRequest request = (CompleteMultipartUploadRequest) context.request();
        logger.debug(
            "key {} uploadId {} complete multipart upload started",
            request.key(),
            request.uploadId());
      }
    }

    @Override
    public void afterExecution(
        Context.AfterExecution context, ExecutionAttributes executionAttributes) {
      if (context.request() instanceof CompleteMultipartUploadRequest) {
        CompleteMultipartUploadRequest request = (CompleteMultipartUploadRequest) context.request();
        logger.debug(
            "key {} uploadId {} complete multipart upload finished, http status {}",
            request.key(),
            request.uploadId(),
            context.httpResponse().statusCode());
      }
    }
  }

  /**
   * Build the endpoint URI for a custom (non-AWS) S3 endpoint such as MinIO or Ozone s3g. If the
   * configured endpoint has no scheme, derive http/https from {@link Constants#SECURE_CONNECTIONS}.
   */
  private static URI getS3Endpoint(String endpoint, Configuration conf) {
    if (!endpoint.contains("://")) {
      boolean secure =
          conf.getBoolean(Constants.SECURE_CONNECTIONS, Constants.DEFAULT_SECURE_CONNECTIONS);
      endpoint = String.format("%s://%s", secure ? "https" : "http", endpoint);
    }
    return URI.create(endpoint);
  }
}
