import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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
    EC2Object ec2;
    boolean toTerminate;
    String inputFilename;
    String bucketName;
    String workerUserData;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, String inputFileName, String workerUserData) {
        this.queue = new Queue();
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.myQueueUrl1 = myQueueUrl1;
        this.InputFileObjectById = inputFileObjectById;
        this.inputFilename = inputFileName;
        this.bucketName = s3.getBucketName();
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.workerUserData = workerUserData;
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
                if (numberOfTasks % 10 == 0) {
                    ec2.createInstance(1,1,this.workerUserData);
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
}
