import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
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
    AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    String inputFilename;
    String workerUserData;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, String inputFileName, AtomicInteger numberOfTasks, String workerUserData) throws Exception {
        System.out.println("the recieving mtasks queue is " + myQueueUrl1);
        this.queue = new Queue();
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.myQueueUrl1 = myQueueUrl1;
        this.InputFileObjectById = inputFileObjectById;
        this.inputFilename = inputFileName;
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.numberOfTasks = numberOfTasks;
        this.workerUserData = workerUserData;
    }

    public void run() {

        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        System.out.println("In InputThread: " + Thread.currentThread());



            InputFileObject currFileObject = new InputFileObject(idOfInputFile.incrementAndGet(), inputFilename, path);
            InputFileObjectById.putIfAbsent(idOfInputFile.get(), currFileObject); //add the currFileObject with his special id
            System.out.println("Successfully added a new file object: " + InputFileObjectById.contains(currFileObject));

            try {
                // Check if need to create worker

                System.out.println("Downloading an object with key: " + inputFilename);
                S3Object object = s3.downloadObject(inputFilename); //input file
                BufferedReader inputFileFromLocalApp = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                System.out.println("file to create tasks from:" + inputFilename);
                /*BufferedReader inputFileFromLocalApp =  new BufferedReader(new FileReader(inputFilename));*/
                String currLine = "";
                String job = "";

                while ((currLine = inputFileFromLocalApp.readLine()) != null) {
                      if (numberOfTasks.get() % 80 == 0) {
                    Instance instance = ec2.createInstance(1,1,workerUserData).get(0);
                    ec2.attachTags(instance,"worker");
                    System.out.println("created new worker instance: " + instance.getInstanceId());
                        }
                    System.out.println("current number of tasks is: " + numberOfTasks);
                    System.out.println(" Making a job from the current read line: " + currLine);
                    // Line content: (obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n"); // added rating******
                    currFileObject.increaseInputLines();
                    job = idOfInputFile + "@" + currLine;
                    queue.sendMessage(myQueueUrl1, job);
                    System.out.println("sending a task to the queue" + myQueueUrl1);
                    numberOfTasks.incrementAndGet();
                    System.out.println("Input id: " + currFileObject.getId() + "number of read line :" + currFileObject.getInputLines() + " number of tasks "+ numberOfTasks );

                }
                currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                System.out.println( "we finish to read all lines :" + currFileObject.getRedAllLines() );

            }
            catch (Exception e) {
                e.printStackTrace(); }


    }
}
