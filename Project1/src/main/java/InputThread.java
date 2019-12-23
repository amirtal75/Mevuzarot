import com.amazonaws.services.ec2.model.Instance;
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
    String QueueUrlLocalApps;
    List<Message> currMessageQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
    S3Bucket s3;
    String myQueueUrl1; //queue for inputJobs
    String myQueueUrl2; //queue for inputJobs
    InputFileObject currFileObject; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    BufferedReader bufferedReader;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, String myQueueUrl2, InputFileObject currFileObject,BufferedReader bufferedReader, AtomicInteger numberOfTasks) {
        this.queue = new Queue();
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.myQueueUrl1 = myQueueUrl1;
        this.currFileObject = currFileObject;
        this.bufferedReader = bufferedReader;
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.numberOfTasks = numberOfTasks;
        this.myQueueUrl2 = myQueueUrl2;
    }

    public void run() {

        //System.out.println("In InputThread: " + Thread.currentThread());
        String delimiter = " -@@@@@@@- ";
            
        

        try {
            // Check if need to create worker
            String currLine = "";
            String job = "";

            while ((currLine = bufferedReader.readLine()) != null) {
                //System.out.println("inside input thread: " + Thread.currentThread().getId() + "\nworking on the file: " + currFileObject.getInputFilename());


                synchronized (this) {
                    int instanceSize = ec2. getInstances("").size();
                    int tasknumber = numberOfTasks.get();
                    if ( tasknumber% 80 == 0 && (instanceSize - 1) <= (tasknumber / 80)) {
                        createworker(myQueueUrl1, myQueueUrl2, tasknumber,instanceSize);
                    }
                    job = currFileObject.getId() + delimiter + currLine;
                    queue.sendMessage(myQueueUrl1, job);
                }

                ////System.out.println(" Making a job from the current read line: " + currLine);
                // Line content: (obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() +  + obj.getReview().getLink() +"\n"); // added rating******



                ////System.out.println("sending a task to the queue" + myQueueUrl1);
                synchronized (this) {
                    currFileObject.increaseInputLines();
                    numberOfTasks.incrementAndGet();
                    //System.out.println("Input id: " + currFileObject.getId() + " number of read line :" + currFileObject.getInputLines() + " number of tasks "+ numberOfTasks );
                    currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                    //System.out.println( "we finish to read all lines :" + currFileObject.getRedAllLines() );
                }

            }


        }
        catch (Exception e) {
            e.printStackTrace(); 
        }

        //System.out.println("InputThread: " + Thread.currentThread() + " finished running");
    }

    public static void createworker(String myQueueUrl1, String myQueueUrl2, int numberOfTasks, int instanceSize){

        System.out.println("\n\n\n\n\ncreating worker when the current number of instances is: " + instanceSize);
        System.out.println("current number of tasks is: " + numberOfTasks);
        EC2Object ec2 = new EC2Object();
        if (instanceSize > 16){
            return;
        }

        if ((numberOfTasks % 80) != 0 && (instanceSize-1) > (numberOfTasks / 80)){
            return;
        }

        try {

            // create user data dor workers
            String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
            String unzip = getProject + "sudo unzip -o master.zip\n";
            String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
            String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
            String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
            String buildProject = setWorkerPom + "sudo mvn -T 4 install -o\n";
            String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

            String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
            String pushFirstArg = createWorkerArgsFile + "echo " + myQueueUrl1 + " >> src/main/java/workerArgs.txt\n";
            String filedata = pushFirstArg + "echo " + myQueueUrl2 + " >> src/main/java/workerArgs.txt\n";

            String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

            // to save time receiving tasks we start one worker
            Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
            ec2.attachTags(instance, "worker");
            System.out.println("created new worker instance: " + instance.getInstanceId());
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
