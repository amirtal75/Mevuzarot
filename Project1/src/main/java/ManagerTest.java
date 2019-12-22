import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class ManagerTest {

    public static void main(String[] args) throws Exception {
        //BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/log.txt"));;

        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        // Read the Queue names from the managerArgs file
        /*try{
            reader = new BufferedReader(new FileReader("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/managerArgs.txt"));
            System.out.println("test");
            QueueUrlLocalApps = reader.readLine();
            summeryFilesIndicatorQueue = reader.readLine();
            System.out.println("the local queue adress is : " + QueueUrlLocalApps);
        } catch (IOException e){
            System.out.println(e.getMessage());
        }*/
        /*System.out.println();
        System.out.println("In Manager:");
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);*/

        // Variables Creation
        boolean shouldTerminate = false;
        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer, InputFileObject>();
        ArrayList<InputFileObject> InputFileObjectList = new ArrayList<InputFileObject>();//????
        ConcurrentHashMap<Integer, StringBuilder> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        Queue queue = new Queue();
        S3Bucket s3 = new S3Bucket();
        EC2Object ec2 = new EC2Object();

        // Create Queues
        String myQueueUrl1 = queue.createQueue(); //queue for inputTask for workers
        String myQueueUrl2 = queue.createQueue();//queue for outputTask from workers
        WorkerTest workerTest = new WorkerTest(myQueueUrl1, myQueueUrl2);
        System.out.println("Worker Receiving Queue: " + myQueueUrl1 + ", Task Results Queue: " + myQueueUrl2);

        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install\n";
        String createAndRunProject = "sudo java -jar target/core-java-1.0-SNAPSHOT.jar\n";

        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg =  createWorkerArgsFile + "echo " + myQueueUrl1 + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + myQueueUrl2 + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n"+ "cd home/ubuntu/\n" + buildProject + filedata;
        System.out.println("Worker UserData: " + workerUserData);

        // Create Thread Pools
        System.out.println("Creating pools for Input Thread & Output Thread");
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;



            try {

                poolForInput.execute(new InputThread(QueueUrlLocalApps, myQueueUrl1, InputFileObjectById, "/home/amirtal/IdeaProjects/Project1/src/main/java/inputFile1.txt2ce70f41-021d-4c12-9db6-5c814212b313.txt", new AtomicInteger(0),workerUserData);
                // Might need to add future
                System.out.println("sleeping 60 sec for input thread to finish");
                Thread.sleep(10000);
                workerTest.main();
                Thread.sleep(25000);
                poolForOutput.execute(new OutputThreadTest(myQueueUrl2, InputFileObjectById, QueueUrlLocalApps, summeryFilesIndicatorQueue));
                System.out.println("Received result from input thread, we need to delete the message");

            } catch (Exception e){
                //System.out.println(e.getMessage());
            }


        /*poolForInput.shutdown();
        poolForOutput.shutdown();*/
    }

}

