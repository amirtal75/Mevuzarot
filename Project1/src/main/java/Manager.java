import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        // Read the Queue names from the managerArgs file
        try{
            reader = new BufferedReader(new FileReader("managerArgs"));
            QueueUrlLocalApps = reader.readLine();
            summeryFilesIndicatorQueue = reader.readLine();
        } catch (IOException e){
            System.out.println(e.getMessage());
        }

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

        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "cp workerpom pom.xml\n";
        String buildProject = setWorkerPom + "mvn compile\n mvn package\n mvn install\n";
        String createAndRunProject = buildProject + "java -jar  target/maven-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
        String pushFirstArg = createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + createAndRunProject + filedata;
        System.out.println("Worker UserData: " + workerUserData);

        String myQueueUrl1 = queue.createQueue(); //queue for inputTask for workers
        String myQueueUrl2 = queue.createQueue();//queue for outputTask from workers
        System.out.println("Worker Receiving Queue: " + myQueueUrl1 + ", Task Results Queue: " + myQueueUrl2);

        System.out.println("Creating pools for Input Thread & Output Thread");
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;

        while (!shouldTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 30); // check about visibility
            } catch (Exception e) {
                e.printStackTrace();
            }

            Message currMessege = currMessageQueue.get(0);
            String messageContent = currMessege.getBody();
            System.out.println("Received Message contents:" + messageContent);

            Future<Message> result = (Future<Message>) poolForInput.submit(new InputThread(QueueUrlLocalApps, myQueueUrl1, InputFileObjectById, messageContent, workerUserData, currMessege));
            // Might need to add future
            poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, stringResultsById, QueueUrlLocalApps));

            if (result != null)
                queue.deleteMessage(myQueueUrl1, result.get()); // result = currMessage
        }

        poolForInput.shutdown();
        poolForOutput.shutdown();
    }
}

