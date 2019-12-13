import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class InputThread implements Runnable {

    Queue queue;
    String location;
    String QueueUrlLocalApps;
    List<Message> currMessageQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
    S3Bucket s3;
    String myQueueUrl1; //queue for inputJobs
    static AtomicInteger idOfInputFile = new AtomicInteger(0);
     ConcurrentHashMap<Integer,InputFileObject> InputFileObjectById; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    static int numberOfTasks = 0;
    AmazonEC2 ec2;
    boolean toTerminate;
    String inputFilename;
    String bucketName;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, AWSCredentialsProvider credentialsProvider, String inputFileName, String bucketName) {
        this.queue = new Queue(credentialsProvider);
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket("assignment1", credentialsProvider);
        this.myQueueUrl1 = myQueueUrl1;
        this.InputFileObjectById = inputFileObjectById;
        this.inputFilename = inputFileName;
        this.bucketName = bucketName;
        this.ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        toTerminate = false;
    }

    public void run() {
        String currMessageRecieptHandle; // we need to hold a String for deleting the current message each time when we finish
        while(!toTerminate) {
//            try {
//                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 30); // check about visibility
//            }
//            catch (Exception e) {
//                e.printStackTrace(); }
//
//            Message currMessege = currMessageQueue.get(0);
//            String[] content = currMessege.getBody().split("@");
//            String inputFilename = content[0];
//            String bucketName = content[1];
            InputFileObject currFileObject = new InputFileObject(idOfInputFile.getAndIncrement(),inputFilename);
            InputFileObjectById.putIfAbsent(idOfInputFile.get(), currFileObject); //add the currFileObject with his special id
            System.out.println("Downloading an object with key: " + inputFilename + " from the bucket: " + bucketName);

            try {
                // Check if need to create worker
                if (numberOfTasks % 100 == 0) {
                    ArrayList<Instance> Ids = getInstances(ec2);
                    String userdata = "";
                    createInstance(Ids, ec2, 1, 1, userdata);
                }

                S3Object object = s3.downloadObject(inputFilename); //input file
                BufferedReader inputFileFromLocalApp = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                String currLine = "";
                String job = idOfInputFile + "@" + currLine;
                while ((currLine = inputFileFromLocalApp.readLine()) != null) {
                    currFileObject.increaseInputLines();
                    job = idOfInputFile + "@" + currLine;
                    queue.sendMessage(myQueueUrl1, job);
                }
                currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
//                currMessageRecieptHandle = currMessege.getReceiptHandle();

//              queue.deleteMessage(myQueueUrl1,currMessege);
                //need to delete
            }
            catch (Exception e) {
                e.printStackTrace(); }
        }
    }

    private static void createInstance(ArrayList<Instance> Ids, AmazonEC2 ec2, int min, int max, String userdata){
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", min, max);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.withUserData(userdata);
        RunInstancesResult instancesResult = ec2.runInstances(request);
    }

    private static ArrayList<Instance> getInstances(AmazonEC2 ec2){
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        boolean notdone = true;

        ArrayList<Instance> Ids= new ArrayList<>();
        while(notdone) {
            DescribeInstancesResult response = ec2.describeInstances(request);
            List<Reservation> reservations = response.getReservations();
            if (reservations.isEmpty()){
                notdone = false;
            }
            else{
                for(Reservation reservation : reservations) {
                    //System.out.println(reservations.size());
                    List<Instance> instances = reservation.getInstances();
                    if (instances.isEmpty()){
                        notdone = false;
                    }
                    else{
                        //System.out.println(instances.size());
                        Ids.addAll(instances);
                    }
                }
            }


            request.setNextToken(response.getNextToken());

            if(response.getNextToken() == null) {
                notdone = false;
            }
        }
        return  Ids;
    }
}
