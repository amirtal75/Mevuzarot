import com.amazonaws.services.ec2.model.Instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class InputThread implements Runnable {

    String workerJobQueue = "workerJobQueue";
    Queue queue;
    S3Bucket s3;
    InputFileObject currFileObject; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger  numberOfTasks;
    EC2Object ec2;

    public InputThread(InputFileObject currFileObject, AtomicInteger  numberOfTasks) {
        this.queue = new Queue();
        this.s3 = new S3Bucket();
        this.currFileObject = currFileObject;
        this.ec2 = new EC2Object();
        this.numberOfTasks = numberOfTasks;
    }

    public void run() {

        String delimiter = " -@@@@@@@- ";
        String originator = "InputThread: " + Thread.currentThread().getId() + " initiating the following task for input file:" + currFileObject.getInputFileID()+ "\n";

        String currLine = "";
        String job = "";
        boolean readAllLines = false;
        while (!readAllLines) {

            //System.out.println("inside input thread: " + Thread.currentThread().getId() + "\nworking on the file: " + currFileObject.getInputFilename());


            createworker(ec2, numberOfTasks.get());
            job = currFileObject.getInputFileID() + delimiter + currLine;
            queue.sendMessage(workerJobQueue, job);
            numberOfTasks.incrementAndGet();


            synchronized (currFileObject){
                currFileObject.readLine();
                if (currLine != null){
                    System.out.println("\n" + originator + " is increasing input lines of: " + currFileObject.getInputFileID() + "from: " + currFileObject.getInputLines());
                    currFileObject.increaseInputLines();
                    currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                    readAllLines = currFileObject.getRedAllLines();
                    System.out.println("to: " + currFileObject.getInputLines() + " and all lines read status = " + readAllLines + "\n");
                }
                // //System.out.println(" Making a job from the current read line: " + currLine);
                // Line content: (obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() +  + obj.getReview().getLink() +"\n"); // added rating******
            }
        }

        System.out.println("InputThread: " + Thread.currentThread() + " finished running");
    }

    public void createworker(EC2Object ec2, int numberOfTasks){

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

        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg = createWorkerArgsFile + "echo " + "workerJobQueue" + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + "completedTasksQueue" + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");

    }
}