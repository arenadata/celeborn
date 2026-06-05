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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.Assert;
import org.junit.Test;

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
        new S3MultipartUploadHandlerSharedState(fs, "test-bucket", 5, 100, 20000)) {
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
        new S3MultipartUploadHandlerSharedState(fs, "test-bucket", 5, 100, 20000)) {
      Assert.assertNotNull(state);
    }
  }
}
