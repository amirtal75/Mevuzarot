
import java.io.*;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class S3Bucket {

    private AmazonS3 s3 = null;
    private String bucketName;
    private String directoryName;


    public S3Bucket() throws Exception {
        this.directoryName = "assigment1";
        this.s3 = AmazonS3ClientBuilder.defaultClient();
        this.bucketName = "amirandamitassignment";
        createBucket();
        System.out.println("The Following bucket was created  " + this.bucketName);
    }


    public
    AmazonS3 getS3() {
        return s3;
    }

    public
    String getBucketName() {
        return bucketName;
    }

    public
    String getDirectoryName() {
        return directoryName;
    }

    public void createBucket() throws Exception {

        createBucket(this.bucketName);
    }

    public void createBucket(String newBucket) throws Exception {

        if (s3.doesBucketExistV2(newBucket)){
            return;
        }
        try {
            System.out.println("Creating bucket " + newBucket + "\n");
            s3.createBucket(newBucket);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void listBuckets() throws Exception {
        try {
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void upload(String filename) throws Exception {

        String key = null;
        try {

            System.out.println("Uploading a new object to S3 from a file\n");
            System.out.println("bucket: " + this.bucketName + ", key: " + filename);
            key = filename.replace('\\', 'a').replace('/','a').replace(':', 'a');

            PutObjectRequest req = new PutObjectRequest(this.bucketName, key, new File(filename));
            s3.putObject(req);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public S3Object downloadObject(String key) throws Exception {
        S3Object answer = null;
        try {
            System.out.println("Downloading an object");
            System.out.println("download file name: " + key);
            answer = s3.getObject(new GetObjectRequest(this.bucketName, key));
        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }

        return answer;
    }

    /**
     * List objects in your bucket by prefix - There are many options for
     * listing the objects in your bucket.  Keep in mind that buckets with
     * many objects might truncate their results when listing their objects,
     * so be sure to check if the returned object listing is truncated, and
     * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
     * additional results.
     */
    public void listObjects() throws Exception {

        try {
            System.out.println("Listing objects");
            ObjectListing objectListing = this.s3.listObjects(new ListObjectsRequest()
                    .withBucketName(this.bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void cleanBucket() throws Exception {

        try {
            System.out.println("deleting objects");
            ObjectListing objectListing = this.s3.listObjects(new ListObjectsRequest()
                    .withBucketName(this.bucketName));

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                deleteObject(objectSummary.getKey());
            }
            System.out.println();

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void deleteObject(String key) throws Exception {
        try {
            System.out.println("Deleting an object\n");
            this.s3.deleteObject(this.bucketName, key);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void deleteBucket() throws Exception {
        try {
            System.out.println("Deleting bucket " + this.bucketName + "\n");
            this.s3.deleteBucket(this.bucketName);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void getTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    private void printServiceError(AmazonServiceException ase){
        System.out.println("Caught an AmazonServiceException, which means your request made it " +
                "to Amazon SQS, but was rejected with an error response for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
    }
    private void printClientError(AmazonClientException ace){
        System.out.println("Caught an AmazonClientException, which means the client encountered " +
                "a serious internal problem while trying to communicate with SQS, such as not " +
                "being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }

}