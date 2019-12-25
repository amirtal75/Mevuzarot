import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import software.amazon.ion.NullValueException;

import java.io.File;

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

    public S3Bucket() {

        try{
            this.s3 = AmazonS3ClientBuilder.defaultClient();
            this.bucketName = "amirandamitassignment";
            createBucket();
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
    }


    public AmazonS3 getS3() {
        return s3;
    }

    public String getBucketName() {return bucketName;}

    public void createBucket() {
        createBucket(bucketName);
    }

    public void createBucket(String newBucket){

        if (!s3.doesBucketExistV2(newBucket)) {
            try {
                s3.createBucket(newBucket);
                System.out.println("Created Bucket Details: \n" + bucketName);

            } catch (AmazonServiceException ase) {
                printServiceError(ase);

            } catch (AmazonClientException ace) {
                printClientError(ace);
            }
        }
    }

    public void listBuckets(){
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

    public void upload(String path, String filename){
        System.out.println("trying to upload the file: " + filename);
        System.out.println(" to the bucket" + this.bucketName);

        try {

            System.out.println("Uploading a new object to S3 from a file\n");
            System.out.println("bucket: " + this.bucketName + ", key: " + filename);
            filename = filename.replace('\\', 'a').replace('/','a').replace(':', 'a');

            PutObjectRequest req = new PutObjectRequest(this.bucketName, filename, new File(path + filename));

            s3.putObject(req);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public S3Object downloadObject(String key) {
        S3Object answer = null;
        try {
            System.out.println("Downloading an object");
            System.out.println("download file name: " + key);
            answer = s3.getObject(new GetObjectRequest(this.bucketName, key));
        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        } catch (NullValueException e){
            e.printStackTrace();
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
    public void listObjects() {

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

    public void cleanBucket() {

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteObject(String key){
        try {
            System.out.println("Deleting an object\n");
            this.s3.deleteObject(this.bucketName, key);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void deleteBucket(){
        try {
            System.out.println("Deleting bucket " + this.bucketName + "\n");
            this.s3.deleteBucket(this.bucketName);

        }  catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    private void printServiceError(AmazonServiceException ase){
        System.out.println("Caught an AmazonServiceException, which means your request made it " +
                "to Amazon S3, but was rejected with an error response for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
    }
    private void printClientError(AmazonClientException ace){
        System.out.println("Caught an AmazonClientException, which means the client encountered " +
                "a serious internal problem while trying to communicate with S3, such as not " +
                "being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }

}