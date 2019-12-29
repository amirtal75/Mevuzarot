import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

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

    public void purgeQueue(String queueName){
        String queueURL = "https://sqs.us-west-2.amazonaws.com/002041186709/" + queueName;
        PurgeQueueRequest purgeQueueRequest = new PurgeQueueRequest().withQueueUrl(queueURL);
        try{
            sqs.purgeQueue(purgeQueueRequest);
        } catch (Exception e){
            if (e.getMessage().contains("Only one PurgeQueue operation on QueueUrlLocalApps is allowed every 60 seconds")){
                System.out.println("The queue was alredy purged in the last minute and we need to wait 60 seconds");
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException ex) {
                    System.out.println("Purge Queue Exception");
                    ex.printStackTrace();
                }
                sqs.purgeQueue(purgeQueueRequest);
            }
        }
    }

    public String createQueue(String queueName) {

        String queueUrl = "";
        try {
            System.out.println("Creating a new SQS queue called " + queueName);
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            return this.sqs.createQueue(createQueueRequest).getQueueUrl();

        } catch (Exception ase) {
            ase.printStackTrace();
        }
        return queueUrl;
    }

    public
    List<String> listQueue() {

        return sqs.listQueues("").getQueueUrls();
    }

    public void sendMessage(String queueUrl, String message) {
        try {
            SendMessageResult sendMessageResult = this.sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        } catch (AmazonServiceException ase) {

        } catch (AmazonClientException ace) {
        }
    }

    public List<Message> recieveMessage(String queueUrl, int numOfMessages, int Visibility){

        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setMaxNumberOfMessages(numOfMessages);
            receiveMessageRequest.setVisibilityTimeout(Visibility);
            List<Message> messages = this.sqs.receiveMessage(receiveMessageRequest).getMessages();
            return messages;
        } catch (AmazonServiceException ase) {
            System.out.println(Thread.currentThread().getId() + "has failed to receive message");
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            System.out.println(Thread.currentThread().getId()  + "has failed to receive message");
            printClientError(ace);
        }
        return null;
    }

    public void printMessage(Message message){
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

    public void deleteMessage(String queueUrl, Message message) {
        try {
            String messageRecieptHandle = message.getReceiptHandle();
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, messageRecieptHandle);
            DeleteMessageResult deleteMessageResult = this.sqs.deleteMessage(deleteMessageRequest);


        } catch (AmazonServiceException ase) {
            System.out.println("\nfailed to delete message from the queue: " + queueUrl);
            printServiceError(ase);

        } catch (AmazonClientException ace) {
            System.out.println("\nfailed to delete message from the queue: " + queueUrl);
            printClientError(ace);

        }
    }

    public List<String> getQueueList(){
        List<String> queues = new ArrayList<>();
        try {
            queues =  sqs.listQueues().getQueueUrls();
        } catch (Exception e){

        }
        return queues;
    }

    public void deleteQueue(String queueUrl, String processID){

        try {
            System.out.println(processID + " Deleting the queue: " + queueUrl + ".\n");
            this.sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        } catch (AmazonServiceException ase) {
            System.out.println("failed to delete  the queue: " + queueUrl);
            printServiceError(ase);
        } catch (AmazonClientException ace) {
            System.out.println("failed to delete  the queue: " + queueUrl);
            printClientError(ace);
        }
    }

    public boolean queueExists(String queueName){
        boolean queueExist = false;
        for (String url:
                new Queue().getQueueList()) {
            if (url.contains(queueName)) {
                queueExist = true;
            }
        }
        return queueExist;
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
