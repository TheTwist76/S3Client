package s3client;
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;


import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY;

public class S3LoadClient {

    private static final Logger logger = LogManager.getLogger(S3LoadClient.class.getName());


    private static AmazonS3 s3Client;

    /**
     * Erzeugt einen neuen S3LoadClient
     *
     * @param endpoint  der Endpunkt zum S3 Storage
     * @param accessKey der AccessKey für den Zugang zum S3 Storage
     * @param secretKey der SecretKey für den Zugang zum S3 Storage
     * @throws RuntimeException Runtime Exception
     */
    public S3LoadClient(String endpoint, String accessKey, String secretKey) throws RuntimeException {

        AWSCredentials credentials;
        credentials = null;


        try {
            credentials = getMyCredentials(accessKey, secretKey);

        } catch (Exception e) {
            throw new RuntimeException("Credentials are incomplete", e);
        }

        try {

            // Client Configuration für die Retry Option
            // Wie soll in einem Fehlerfall reagiert werden?
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.withRetryPolicy(new RetryPolicy(new CustomRetryCondition(),
                    DEFAULT_BACKOFF_STRATEGY,
                    DEFAULT_MAX_ERROR_RETRY,
                    false));

            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_WEST_2.name()))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(configuration)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();


        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw new RuntimeException("Cannot load AmazonS3ClientBuilder", ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw new RuntimeException("Cannot load AmazonS3ClientBuilder", ace);
        }
    }

    /**
     * Erzeugt AWSCredentials aus den übergebenen Informationen
     *
     * @param accessKey     der AccessKey für den Zugang zum S3 Storage
     * @param secretKey der SecretKey für den Zugang zum S3 Storage
     * @throws RuntimeException Runtime Exception
     */
    private static AWSCredentials getMyCredentials(String accessKey, String secretKey) throws RuntimeException {

        if (!StringUtils.isNullOrEmpty(accessKey) && !StringUtils.isNullOrEmpty(secretKey)) {
            return new BasicAWSCredentials(accessKey, secretKey);
        } else {
            throw (new RuntimeException("accessKey and/or secretKey null or empty"));
        }
    }


    /**
     * Custom retry condition with exception logs.
     */
    public static class CustomRetryCondition implements RetryPolicy.RetryCondition {

        @Override
        public boolean shouldRetry(AmazonWebServiceRequest originalRequest,
                                   AmazonClientException exception,
                                   int retriesAttempted) {

            logger.info("Encountered exception " + exception + "  for request " + originalRequest + " , retries attempted: " + retriesAttempted);


            // Always retry on client exceptions caused by IOException
            if (exception.getCause() instanceof IOException) {
                return true;
            }

            // Only retry on a subset of service exceptions
            if (exception instanceof AmazonServiceException) {
                AmazonServiceException ase = (AmazonServiceException) exception;

                /*
                 * For 500 internal server errors and 503 service unavailable errors, we want to
                 * retry, but we need to use an exponential back-off strategy so that we don't
                 * overload a server with a flood of retries.
                 */
                if (RetryUtils.isRetryableServiceException(new SdkBaseException(ase))) {
                    return true;
                }

                /*
                 * Throttling is reported as a 400 error from newer services. To try and smooth out
                 * an occasional throttling error, we'll pause and retry, hoping that the pause is
                 * long enough for the request to get through the next time.
                 */
                if (RetryUtils.isThrottlingException(new SdkBaseException(ase))) {
                    return true;
                }

                /*
                 * Clock skew exception. If it is then we will get the time offset between the
                 * device time and the server time to set the clock skew and then retry the request.
                 */
                return RetryUtils.isClockSkewError(new SdkBaseException(ase));
            }

            return false;
        }
    }

    /**
     * liest den Inputstream aus und schreibt ihn zu Testzwecken auf stdout
     *
     * @param input    der Inputstream
     * @param ausgeben true falls Ausgabe auf stdout zu Testzwecken erwünscht
     * @throws IOException Datei kann nicht gelesen werden
     */
    private static void displayTextInputStream(InputStream input, boolean ausgeben) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;

            if (ausgeben) {
                System.out.println("    " + line);
            }
        }
        if (ausgeben) {
            System.out.println();
        }

    }

    /**
     * Legt ein neues Bucket an sofern dieses noch nicht existiert
     *
     * @param bucketname       Der Name des Buckets
     * @param enableVersioning True wenn ein Bucket mit Versionierung erstellt werden soll
     * @throws Exception Exception
     */
    public void createBucket(String bucketname, boolean enableVersioning) throws Exception {

        try {
            if (s3Client.doesBucketExistV2(bucketname)) {
                logger.info("Bucket " + bucketname + " already exists.");
            } else {
                try {
                    logger.info("Creating bucket " + bucketname);
                    s3Client.createBucket(bucketname);

                    if (enableVersioning) {
                        logger.info("Bucket versioning is: true ");
                        BucketVersioningConfiguration configurationBucket = new BucketVersioningConfiguration()
                                .withStatus("Enabled");
                        SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = new SetBucketVersioningConfigurationRequest(
                                bucketname, configurationBucket);
                        s3Client.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);

                    } else {
                        logger.info("Bucket versioning is: false ");
                    }

                    logger.info("Bucket created with the name: " + bucketname);
                } catch (AmazonS3Exception e) {
                    logger.error(e.getErrorMessage());
                    throw e;
                }
            }
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw new RuntimeException("Cannot create Bucket", ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw new RuntimeException("Cannot create Bucket", ace);
        }
    }

    /**
     * Überträgt ein Objekt auf das Storage
     *
     * @param bucket     Der Name des Buckets
     * @param objectname Der ObjektName des Objekts im Storage
     * @param filename   Dateiname des zu übertragenden Objekts
     * @throws IOException IOException
     */
    public void putObject(String bucket, String objectname, String filename)
            throws AmazonServiceException, AmazonClientException, Exception {

        try {
            logger.info("Uploading a new object to S3 from a file");

            // String key_name = Paths.get(file_path).getFileName().toString();
            PutObjectResult result = s3Client.putObject(bucket, objectname, new File(filename));

            logger.info("File Information:");
            logger.info("Objectname is: " + objectname);
            logger.info("Version id of the object is: " + result.getETag());


            logger.info("Upload completed");

        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw (ace);
        }
    }

    /**
     * Löscht eine Objekt aus einem nicht versioniertem Bucket
     *
     * @param bucketName Der Name des Buckets
     * @param objectName Der ObjektName des Objekts im Storage
     * @throws Exception Exception
     */
    public void deleteObjectNonVersionedBucket(String bucketName, String objectName)
            throws AmazonServiceException, SdkClientException, Exception {

        try {
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, objectName));

        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (SdkClientException sce) {
            logger.error("Caught an SDKClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + sce.getMessage());
            throw (sce);
        }
    }

    /**
     * Löscht eine Version eines Objekts aus einem versioniertem Bucket
     *
     * @param bucketName Der Name des Buckets
     * @param objectName Der ObjektName des Objekts im Storage
     * @param versionsid Versionsid des zu löschenden Objekts
     * @throws Exception
     */
    public void deleteObjectVersionEnabledBucket(String bucketName, String objectName, String versionsid)
            throws AmazonServiceException, SdkClientException, Exception {

        try {
            // Check to ensure that the bucket is versioning-enabled.
            String bucketVersionStatus = s3Client.getBucketVersioningConfiguration(bucketName).getStatus();
            if (!bucketVersionStatus.equals(BucketVersioningConfiguration.ENABLED)) {
                logger.info("Bucket " + bucketName + " is not versioning-enabled.");
            } else {
                // Delete the version of the object that we just created.
                logger.info("Deleting versioned object " + objectName);
                s3Client.deleteVersion(new DeleteVersionRequest(bucketName, objectName, versionsid));
                logger.info("Object " + objectName + ", version " + versionsid + " deleted");
            }

        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (SdkClientException sce) {
            logger.error("Caught an SDKClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + sce.getMessage());
            throw (sce);
        }
    }

    /**
     * Löscht ein Bucket und dessen Inhalt alle Objekte und die dazu gehörigen
     * Versionen
     *
     * @param bucketName Der Name des Buckets
     * @throws Exception Exception
     */
    public void deleteBucket(String bucketName) throws AmazonServiceException, AmazonClientException, Exception {

        try {

            String bucketVersionStatus = s3Client.getBucketVersioningConfiguration(bucketName).getStatus();
            if (bucketVersionStatus.equals(BucketVersioningConfiguration.ENABLED)) {
                // Delete all object versions (required for versioned buckets).
                logger.info("All versions and objects are deleted");
                VersionListing versionList = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
                while (true) {
                    for (S3VersionSummary vs : versionList.getVersionSummaries()) {
                        s3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                        logger.info("Delete Object " + vs.getKey() + " with version " + vs.getVersionId());
                    }

                    if (versionList.isTruncated()) {
                        versionList = s3Client.listNextBatchOfVersions(versionList);
                    } else {
                        break;
                    }
                }
            } else {
                // Delete all objects from the bucket. This is sufficient
                // for unversioned buckets. For versioned buckets, when you attempt to delete objects, Amazon S3 inserts
                // delete markers for all objects, but doesn't delete the object versions.
                // To delete objects from versioned buckets, delete all of the object versions before deleting
                // the bucket (see below for an example).
                ObjectListing objectListing = s3Client.listObjects(bucketName);
                logger.info("All objects are deleted");
                while (true) {
                    Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
                    while (objIter.hasNext()) {
                        s3Client.deleteObject(bucketName, objIter.next().getKey());
                        logger.info("Delete Object " + objIter.next().getKey());
                    }

                    // If the bucket contains many objects, the listObjects() call
                    // might not return all of the objects in the first listing. Check to
                    // see whether the listing was truncated. If so, retrieve the next page of objects
                    // and delete them.
                    if (objectListing.isTruncated()) {
                        objectListing = s3Client.listNextBatchOfObjects(objectListing);
                    } else {
                        break;
                    }
                }
            }

            /* Send Delete Bucket Request */
            s3Client.deleteBucket(bucketName);

            // Überprüfung ob das Bucket noch existiert
            if (!(s3Client.doesBucketExistV2(bucketName))) {
                logger.info("Bucket " + bucketName + "  was deleted");
            }

        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw (ace);
        }

    }

    /**
     * Holt ein Objekt aus dem Storage und legt es lokal ab
     *
     * @param bucketName Der Name des Buckets
     * @param objectName Der ObjektName des Objekts im Storage
     * @param fileName   Dateiname für die Ablage des Objekts
     * @throws Exception Exception
     */
    public void getObject(String bucketName, String objectName, String fileName)
            throws AmazonServiceException, AmazonClientException, Exception {
        try {
            System.out.println("Downloading and storing an object");

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectName));
            logger.info("Content-Type: " + object.getObjectMetadata().getContentType());
            // displayTextInputStream(object.getObjectContent());
            File targetFile = new File(fileName);
            java.nio.file.Files.copy(object.getObjectContent(), targetFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
            logger.info("file successfully downloaded and stored");
            // targetFile.delete();
            targetFile = null;

        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw (ace);
        }
    }

    /**
     * Holt ein Objekt aus dem Storage, legt es aber nicht lokal ab
     *
     * @param bucketName Der Name des Buckets
     * @param objectName Der ObjektName des Objekts im Storage *
     * @throws Exception Exception
     */
    public void getObject(String bucketName, String objectName)
            throws AmazonServiceException, AmazonClientException, Exception {
        try {
            logger.info("Downloading an object without storing");

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectName));
            logger.info("Content-Type: " + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent(), false);
            logger.info("file successfully downloaded");
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw (ace);
        }
    }

    /**
     * Holt ein Version eines Objekts aus dem Storage, legt es aber nicht lokal
     * ab
     *
     * @param bucketName Der Name des Buckets
     * @param objectName Der ObjektName des Objekts im Storage
     * @param versionid  Die Versionsid des Objekts im Storage
     * @throws Exception Exception
     */
    public void getObjectVersion(String bucketName, String objectName, String versionid)
            throws AmazonServiceException, AmazonClientException, Exception {
        try {
            logger.info("Downloading an object with storing");

            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectName, versionid));
            logger.info("Content-Type: " + object.getObjectMetadata().getContentType());
            //displayTextInputStream(object.getObjectContent(), false);
            File targetFile = new File(objectName);
            java.nio.file.Files.copy(object.getObjectContent(), targetFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
            logger.info("file successfully downloaded and stored");
            // targetFile.delete();
            targetFile = null;
            logger.info("file successfully downloaded");
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
            throw (ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
            throw (ace);
        }
    }


    /**
     * Listet alle Objekte eines Buckets auf und exportiert diese als File
     *
     * @param bucket_name Der Name des Buckets
     * @param filename    Der ObjektName des Objekts im Storage
     * @throws Exception Exception
     */
    public void listObjects(String bucket_name, String filename) throws Exception {

        String output;
        try {
            // Retrieve the list of versions. If the bucket contains more
            logger.info("List all objects of the bucket \"" + bucket_name + "\" to File \"" + filename + "\"");
            File file = new File(filename);
            FileWriter writer = new FileWriter(file);
            file.createNewFile();
            ListVersionsRequest request = new ListVersionsRequest().withBucketName(bucket_name).withMaxResults(1);
            VersionListing versionListing = s3Client.listVersions(request);
            // int numVersions = 0, numPages = 0;
            while (true) {
                // numPages++;
                for (S3VersionSummary objectSummary : versionListing.getVersionSummaries()) {
                    output = objectSummary.getKey() + ";" + objectSummary.getVersionId() + "\n";
                    writer.write(output);

                }
                // Check whether there are more pages of versions to retrieve.
                // If there are, retrieve them. Otherwise, exit the loop.
                if (versionListing.isTruncated()) {
                    versionListing = s3Client.listNextBatchOfVersions(versionListing);
                } else {
                    writer.flush();
                    writer.close();
                    break;
                }
            }
            logger.info("Exported list finished");
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
        } catch (SdkClientException | IOException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
        }
    }
}
