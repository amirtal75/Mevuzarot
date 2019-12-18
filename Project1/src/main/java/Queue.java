import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/this.sqs.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class Queue {

    public AmazonSQS sqs = null;

    public Queue() {

        this.sqs = AmazonSQSClientBuilder.defaultClient();
    }
    public
    AmazonSQS getSqs() {
        return sqs;
    }

    public String createQueue() throws Exception {

        String queueUrl = "";
        try {
            String queueName = "MyQueue"+ UUID.randomUUID();
            System.out.println("Creating a new SQS queue called " +queueName);
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            return this.sqs.createQueue(createQueueRequest).getQueueUrl();

        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
        return queueUrl;


    }

    public void listQueue() throws Exception {

        try {
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : this.sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();
        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public void sendMessage(String queueUrl, String message) throws Exception {

        try {
            System.out.println("Sending a message to MyQueue.\n");
            this.sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }

    public List<Message> recieveMessage(String queueUrl) throws Exception {

        return recieveMessage(queueUrl, 1, 30);
    }

    public List<Message> recieveMessage(String queueUrl, int numOfMessages, int Visibility) throws Exception {

        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setMaxNumberOfMessages(numOfMessages);
            receiveMessageRequest.setVisibilityTimeout(Visibility);
            List<Message> messages = this.sqs.receiveMessage(receiveMessageRequest).getMessages();
            return messages;
        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
        return null;
    }

    public void printMessage(Message message) throws Exception{
        System.out.println("  Message");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
        for (Entry<String, String> entry : message.getAttributes().entrySet()) {
            System.out.println("  Attribute");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    Value: " + entry.getValue());
        }
        System.out.println();
    }

    public void deleteMessage(String queueUrl, Message message) throws Exception {
        try {
            System.out.println("Deleting a message.\n");
            String messageRecieptHandle = message.getReceiptHandle();
            this.sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));

        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
    }



    public void deleteQueue(String queueUrl){

        try {
            System.out.println("Deleting the queue: " + queueUrl + ".\n");
            this.sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        } catch (AmazonServiceException ase) {
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            printClientError(ace);
        }
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

    private AWSStaticCredentialsProvider getMyCredentials(){
        AWSStaticCredentialsProvider credentialsProvider = null;
        try{
            credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        } catch (Exception e){
            AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion("us-west-2")
                    .build();

            // Start a session.
            GetSessionTokenRequest getSessionTokenRequest = new GetSessionTokenRequest().withDurationSeconds(3500);
            // The duration can be set to more than 3600 seconds only if temporary
            // credentials are requested by an IAM user rather than an account owner.
            AssumeRoleResult sessionTokenResult = stsClient
                    .assumeRole(new AssumeRoleRequest().withRoleArn("arn:aws:iam::002041186709:role/projectRole"));
            System.out.println(sessionTokenResult.getCredentials().getAccessKeyId());
            Credentials sessionCredentials = sessionTokenResult
                    .getCredentials()
                    .withSessionToken(sessionTokenResult.getCredentials().getSessionToken())
                    .withExpiration(sessionTokenResult.getCredentials().getExpiration());

            // Package the temporary security credentials as a BasicSessionCredentials object
            // for an Amazon S3 client object to use.
            BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(
                    sessionCredentials.getAccessKeyId(), sessionCredentials.getSecretAccessKey(),
                    sessionCredentials.getSessionToken());

            credentialsProvider = new AWSStaticCredentialsProvider(basicSessionCredentials);
        }

        return credentialsProvider;
    }

}