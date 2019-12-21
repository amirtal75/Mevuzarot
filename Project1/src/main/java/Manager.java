import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class Manager {

    public static void main(String[] args) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/log.txt"));
        writer.write("test");
        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        // Read the Queue names from the managerArgs file
        try{
            reader = new BufferedReader(new FileReader("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/managerArgs.txt"));
            System.out.println("test");
            QueueUrlLocalApps = reader.readLine();
            summeryFilesIndicatorQueue = reader.readLine();
            System.out.println("the local queue adress is : " + QueueUrlLocalApps);
        } catch (IOException e){
            System.out.println(e.getMessage());
        }
        System.out.println();
        System.out.println("In Manager:");
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

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
        System.out.println("Worker Receiving Queue: " + myQueueUrl1 + ", Task Results Queue: " + myQueueUrl2);

        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

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

        while (!shouldTerminate) {

            try {
                System.out.println("the local queue adress is : " + QueueUrlLocalApps);
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
                if (currMessageQueue.size() > 0){
                    Message currMessege = currMessageQueue.get(0);
                    String messageContent = currMessege.getBody();
                    System.out.println("Received Message contents:" + messageContent);

                    poolForInput.execute(new InputThread(QueueUrlLocalApps, myQueueUrl1, InputFileObjectById, messageContent, workerUserData));
                    // Might need to add future
                    poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, stringResultsById, QueueUrlLocalApps));
                    System.out.println("Received result from input thread, we need to delete the message");
                    queue.deleteMessage(myQueueUrl1, currMessege); // result = currMessag
                    }

                else{
                    Thread.sleep(3000);
                }

            } catch (Exception e){
                //System.out.println(e.getMessage());
            }
        }

        poolForInput.shutdown();
        poolForOutput.shutdown();
    }

}

