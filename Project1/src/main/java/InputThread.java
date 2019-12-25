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
    String completedTasksQueue;
    List<Message> currMessageQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
    S3Bucket s3;
    String workerJobQueue; //queue for inputJobs
    static AtomicInteger idOfInputFile = new AtomicInteger(1);
    ConcurrentHashMap<Integer,InputFileObject> InputFileObjectById; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    String inputFilename;
    BufferedReader inputFileFromLocalApp;

    public InputThread(String workerJobQueue, String completedTasksQueue, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, String inputFileName, AtomicInteger numberOfTasks, BufferedReader inputFileFromLocalApp){
        System.out.println("the recieving mtasks queue is " + workerJobQueue);
        this.queue = new Queue();
        completedTasksQueue = completedTasksQueue;
        this.s3 = new S3Bucket();
        this.workerJobQueue = workerJobQueue;
        this.InputFileObjectById = inputFileObjectById;
        this.inputFilename = inputFileName;
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.numberOfTasks = numberOfTasks;
        this.inputFileFromLocalApp = inputFileFromLocalApp;
    }

    public void run() {
        String delimiter = " -@@@@@@@- ";
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        System.out.println("In InputThread: " + Thread.currentThread());



        InputFileObject currFileObject = new InputFileObject(idOfInputFile.incrementAndGet(), inputFilename);
        InputFileObjectById.putIfAbsent(idOfInputFile.get(), currFileObject); //add the currFileObject with his special id

        try {
            String currLine = "";
            String job = "";

            while ((currLine = inputFileFromLocalApp.readLine()) != null) {
                //System.out.println("inside input thread, numberOfTasks: " + numberOfTasks.get() + "\nnumber wof instances: " + ec2. getInstances("").size());

                // check if more workers are needed
                synchronized (this) {

                    System.out.println("\nThe InputThread: " + Thread.currentThread().getId() + " is about to increase the line");
                    System.out.println("from: "+ currFileObject.getInputLines());

                    createworker(workerJobQueue, completedTasksQueue, ec2, queue, numberOfTasks.get());
                    currFileObject.increaseInputLines();
                    job = idOfInputFile + delimiter + currLine;
                    queue.sendMessage(workerJobQueue, job);
                    numberOfTasks.incrementAndGet();
                    currFileObject.setredAllLinesTrue();
                }
                System.out.println("\nThe InputThread: " + Thread.currentThread().getId() + " completed increasng the line:");
                System.out.println("to: "+ currFileObject.getInputLines());

            }
             // we've finished to read all lines of the input file
        }
        catch (Exception e) {
            e.printStackTrace(); }


    }

    private void createworker(String workerJobQueue, String completedTasksQueue, EC2Object ec2, Queue queue, int numberOfTasks){

        int workerinstances = ec2.getInstances("").size() - 1;
        Boolean tasksDivides = (numberOfTasks % 80) == 0;
        int tasks = numberOfTasks/80;
        Boolean condition = tasksDivides == false && workerinstances <= (tasks);

        if ( condition == false || workerinstances > 15){
            return;
        }
        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";
        System.out.println("inside create worker: " + workerJobQueue);
        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg = createWorkerArgsFile + "echo " + workerJobQueue + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + completedTasksQueue + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");

    }
}
