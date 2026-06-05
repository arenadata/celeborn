# multipart-uploader-s3 — AWS SDK v2 migration (Ozone s3g)

Intermediate working notes for review. Tracks the port of
`multipart-uploader-s3` from the AWS Java SDK **v1** (`com.amazonaws.*`) to the
AWS Java SDK **v2** (`software.amazon.awssdk.*`) so the module lines up with:

- **AWS SDK** `2.35.4` (`aws.java.sdk.v2.version` in the root `pom.xml`)
- **Hadoop** `3.4.3.1-4.3.0-1` (arenadata/hadoop fork)
- **Spark** `3.5.4.4-4.3.0-1` (arenadata/spark fork — build profile `spark-3.5`)

## 1. Why the change is needed

`S3MultipartUploadHandler` was written against SDK v1 (`com.amazonaws.services.s3.AmazonS3`).
The rest of the repo (`master/pom.xml`, `tests/spark-it/pom.xml`) already moved to
SDK v2 (`software.amazon.awssdk`), and the arenadata hadoop-aws `3.4.3.1-4.3.0-1`
jar is compiled against SDK v2 only. Two concrete breakages:

1. `aws-java-sdk-s3` / `aws-java-sdk-sts` v1 artifacts are no longer pulled in by
   the new hadoop-aws; the module referenced an **undefined** `${aws.version}`
   property (only `${aws.java.sdk.v2.version}` exists in the root pom).
2. The v1-only helper `S3AUtils.createAWSCredentialProviderSet(URI, Configuration)`
   **no longer exists** in hadoop-aws v2. The handler and its test call it.

## 2. Third-party store (Ozone s3g) compatibility

The hadoop-aws upgrade to AWS SDK 2.35.4 (HADOOP-19654) is the trigger for the
checksum behaviour below. Newer SDKs (≥ 2.30) changed the default request
integrity protection: the SDK now adds a **CRC32 flexible checksum** (and
trailer) to every request and stops sending `Content-MD5`.
Third-party S3 stores — **Ozone s3g**, MinIO, etc. — reject or mishandle those
requests (e.g. bulk delete failing with a missing-MD5 error).

To restore compatibility, the arenadata hadoop-aws `DefaultS3ClientFactory`
exposes three switches (verified in the shipped sources jar
`hadoop-aws-3.4.3.1-4.3.0-1-sources.jar`, `org/apache/hadoop/fs/s3a/Constants.java`):

| Constant (`Constants.*`) | Config key                   | Default | Effect when true                            |
|--------------------------|------------------------------|---------|---------------------------------------------|
| `REQUEST_MD5_HEADER`     | `fs.s3a.request.md5.header`  | `true`  | add `LegacyMd5Plugin` → sends `Content-MD5` |
| `CHECKSUM_GENERATION`    | `fs.s3a.checksum.generation` | `false` | `RequestChecksumCalculation.WHEN_SUPPORTED` |
| `CHECKSUM_VALIDATION`    | `fs.s3a.checksum.validation` | `false` | `ResponseChecksumValidation.WHEN_SUPPORTED` |

`DefaultS3ClientFactory.configureClientBuilder()` does exactly:

```java
if (parameters.isMd5HeaderEnabled()) {                 // fs.s3a.request.md5.header (default true)
  builder.addPlugin(LegacyMd5Plugin.create());
}
builder.requestChecksumCalculation(                    // fs.s3a.checksum.generation (default false)
    checksumGeneration ? WHEN_SUPPORTED : WHEN_REQUIRED);
builder.responseChecksumValidation(                    // fs.s3a.checksum.validation (default false)
    checksumValidation ? WHEN_SUPPORTED : WHEN_REQUIRED);
```

The defaults (MD5 header **on**, request/response checksum generation **off** →
`WHEN_REQUIRED`) are precisely the Ozone-s3g-friendly combination. The
multipart-uploader builds its **own** `S3Client` (it does not reuse the S3A
filesystem client), so it must reproduce this same configuration, reading the
same hadoop `Configuration` keys, or it will break against Ozone s3g.

## 3. Public API mapping (v1 → v2)

| v1 (`com.amazonaws...`)                                      | v2 (`software.amazon.awssdk...`)                                                     |
|--------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `AmazonS3`                                                   | `services.s3.S3Client`                                                               |
| `AmazonS3ClientBuilder.standard()`                           | `S3Client.builder()`                                                                 |
| `.withCredentials(AWSCredentialsProvider)`                   | `.credentialsProvider(AwsCredentialsProvider)`                                       |
| `.withRegion(String)`                                        | `.region(Region.of(String))`                                                         |
| `.withEndpointConfiguration(new EndpointConfiguration(e,r))` | `.endpointOverride(URI)` + `.region(...)`                                            |
| `.withPathStyleAccessEnabled(b)`                             | `.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(b).build())` |
| `ClientConfiguration` + `RetryPolicy`                        | `.overrideConfiguration(ClientOverrideConfiguration ... retryStrategy)`              |
| `s3.shutdown()`                                              | `s3.close()`                                                                         |
| `initiateMultipartUpload(req)` → `getUploadId()`             | `createMultipartUpload(req)` → `response.uploadId()`                                 |
| `uploadPart(UploadPartRequest.with...InputStream/partSize)`  | `uploadPart(UploadPartRequest, RequestBody.fromInputStream(in, len))`                |
| `listParts(req)` → `PartListing`/`PartSummary`               | `listParts(req)` → `ListPartsResponse`/`Part` (`.nextPartNumberMarker()`)            |
| `PartETag(num, etag)`                                        | `CompletedPart.builder().partNumber(n).eTag(e).build()`                              |
| `completeMultipartUpload(req with List<PartETag>)`           | `completeMultipartUpload(req with CompletedMultipartUpload.parts(...))`              |
| `abortMultipartUpload(req)`                                  | `abortMultipartUpload(req)` (builder form)                                           |
| `AmazonClientException`                                      | `core.exception.SdkException`                                                        |
| `req.withGeneralProgressListener(ProgressListener)`          | client-level `core.interceptor.ExecutionInterceptor` (see §8)                        |
| `S3AUtils.createAWSCredentialProviderSet(uri, conf)`         | `auth.CredentialProviderListFactory.createAWSCredentialProviderList(uri, conf)`      |

`AWSCredentialProviderList` (hadoop-aws v2) implements
`software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`, so it can be
passed straight to `.credentialsProvider(...)`.

## 4. Retry handling

v1 used `PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION` +
`SDKDefaultBackoffStrategy(baseDelay, baseDelay, maxBackoff)` on the client, plus
a hand-rolled retry loop in `complete()`. v2 unifies this into a **single**
client-level retry strategy shared by all operations (`startUpload` / `putPart` /
`listParts` / `complete` / `abort`):

- the builder gets a `StandardRetryStrategy` with
  `maxAttempts(max(1, maxRetries + 1))` — `maxAttempts` counts total tries, so it
  is `retries + 1` — **and** `BackoffStrategy.exponentialDelay(baseDelay, maxBackoff)`
  as both `backoffStrategy` and `throttlingBackoffStrategy`, honouring the
  configured delays (`s3MultiplePartUploadMaxRetries`, `baseDelay`, `maxBackoff`).
- `complete()` issues a **single** `completeMultipartUpload` call and lets the
  client retry it. The old nested manual loop was removed so retries are not
  multiplied (`maxRetries × maxRetries`) for that one operation.

`putPart` retry-safety relies on the caller passing a fully-buffered,
mark/reset-capable `ByteArrayInputStream` — documented on the
`MultipartUploadHandler.putPart` contract.

## 5. Endpoint / region for Ozone s3g

Ozone s3g is reached as an S3-compatible endpoint:

- `fs.s3a.endpoint` = the s3g endpoint (e.g. `http://ozone-s3g:9878`)
- `fs.s3a.path.style.access` = `true`
- `fs.s3a.endpoint.region` = any value (Ozone ignores it; SDK still requires one)

The handler mirrors the previous behaviour: if `fs.s3a.endpoint` is set, use
`endpointOverride` + region + path-style; otherwise just set the region. The
MD5/checksum switches above are applied in **both** branches.

## 6. pom.xml changes

- Drop `com.amazonaws:aws-java-sdk-s3` and `com.amazonaws:aws-java-sdk-sts`.
- Change the hadoop-aws exclusion from `com.amazonaws:aws-java-sdk-bundle` to
  `software.amazon.awssdk:bundle` (the v2 bundle hadoop-aws would otherwise drag in).
- Add `software.amazon.awssdk:s3` and `software.amazon.awssdk:sts`
  (sts needed for `WebIdentityTokenFileCredentialsProvider`), versioned with
  `${aws.java.sdk.v2.version}` (2.35.4).

## 7. Test changes

`S3MultipartUploadHandlerSuiteJ` is updated to:

- call `getCredentialsProvider` which now returns the v2-backed
  `AWSCredentialProviderList`;
- use the v2 provider class name
  `software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`
  and assert against `listProviderNames()`.

## 8. Progress logging (replacing the v1 `ProgressListener`)

v1 attached a `ProgressListener` to the `CompleteMultipartUploadRequest` that
emitted `logger.debug(...)` lines for progress events. It was **debug-only** — no
functional behaviour. v2 request objects have no `withGeneralProgressListener`
equivalent, so it is reproduced with a client-level `ExecutionInterceptor`
(`CompleteUploadProgressLogger`) that:

- is scoped to the complete call by checking
  `context.request() instanceof CompleteMultipartUploadRequest`, and logs
  `key` + `uploadId` (from the request) at start, and `key` + `uploadId` +
  HTTP status at finish;
- is **only registered when `logger.isDebugEnabled()`** at client-build time.
  When debug is off (the normal production case) no interceptor is added at all,
  so the SDK request hot path is untouched and carries zero per-request overhead.

Caveat: `isDebugEnabled()` is evaluated once, when the shared `S3Client` is built
(same as the checksum/md5/retry config). Flipping the log level at runtime
afterwards will not add or remove the interceptor for the life of that client.

## 9. Build / verification

Verified locally with **OpenJDK 17** (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`;
the forked Error Prone compiler needs `JAVA_HOME` pointed at a JDK 17, otherwise it
throws an unhandled exception in `celeborn-common`):

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
./build/mvn -pl multipart-uploader/multipart-uploader-s3 -am \
  -Paws -Pspark-3.5 -Phadoop-3 test
```

Result: `BUILD SUCCESS`, `S3MultipartUploadHandlerSuiteJ` — `Tests run: 2,
Failures: 0, Errors: 0`.
