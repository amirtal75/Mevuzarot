import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


//public class InputThread implements Runnable {
public class InputThread implements Callable<Message> {

    Queue queue;
    String location;
    String QueueUrlLocalApps;
    List<Message> currMessageQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
    S3Bucket s3;
    String myQueueUrl1; //queue for inputJobs
    static AtomicInteger idOfInputFile = new AtomicInteger(0);
     ConcurrentHashMap<Integer,InputFileObject> InputFileObjectById; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    static AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    String inputFilename;
    String workerUserData;
    Message message;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, String inputFileName, String workerUserData,Message message) throws Exception {
        this.queue = new Queue();
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.myQueueUrl1 = myQueueUrl1;
        this.InputFileObjectById = inputFileObjectById;
        this.inputFilename = inputFileName;
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.workerUserData = workerUserData;
        this.message = message;
    }

    public Message call() {
        Message resultMessage = null;
        String currMessageRecieptHandle; // we need to hold a String for deleting the current message each time when we finish
        System.out.println("In InputThread: " + Thread.currentThread());



            InputFileObject currFileObject = new InputFileObject(idOfInputFile.getAndIncrement(), inputFilename);
            InputFileObjectById.putIfAbsent(idOfInputFile.get(), currFileObject); //add the currFileObject with his special id
            System.out.println("Successfully added a new file object: " + InputFileObjectById.contains(currFileObject));

            try {
                // Check if need to create worker

                System.out.println("Downloading an object with key: " + inputFilename);
                S3Object object = s3.downloadObject(inputFilename); //input file

                // S3 download gives an input stream containg the file contents

//                File file = new File("C:\\Users\\amithaim7\\IdeaProjects\\maven\\src\\main\\java\\inputFile1.txt");
//                Scanner sc = new Scanner(file);
//                String line;
//                while (sc.hasNext() ) {
//                    line = sc.nextLine();

                BufferedReader inputFileFromLocalApp = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                String currLine = "";
                String job = "";
                while ((currLine = inputFileFromLocalApp.readLine()) != null) {
                    System.out.println("current number of tasks is: " + numberOfTasks);
                    if (numberOfTasks.get() % 10 == 0) {
                        Instance instance = ec2.createInstance(1,1,this.workerUserData).get(0);
                        System.out.println("created new worker instance: " + instance.getInstanceId());
                    }

                    System.out.println(" Making a job from the current read line: " + currLine);
                    // Line content: (obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n"); // added rating******
                    currFileObject.increaseInputLines();
                    job = idOfInputFile + "@" + currLine;
                    queue.sendMessage(myQueueUrl1, job);
                    numberOfTasks.incrementAndGet();
                }
                currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                System.out.println( "we finish to read all lines :" + currFileObject.getRedAllLines() );
                resultMessage = this.message;
            }
            catch (Exception e) {
                e.printStackTrace(); }

        return resultMessage;
    }
}
