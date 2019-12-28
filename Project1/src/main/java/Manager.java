import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager{

    public static void main(String[] args) throws IOException, InterruptedException {

        String mark = "!!!!!!!!!!!!!!!!!!!";
        System.out.println(mark + " In Manager:" + mark + "\n");

        // Variable Declaration
        S3Bucket s3 = new S3Bucket();
        Queue queue = new Queue();
        EC2Object ec2 = new EC2Object();
        String QueueUrlLocalApps = "QueueUrlLocalApps";
        AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicBoolean continueRunning = new AtomicBoolean(true);
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<String, InputFileObject> InputFileObjectById = new ConcurrentHashMap<>();

        System.out.println("Thread pools creation");
        // Create Thread Pools
        ExecutorService poolForInput = Executors.newCachedThreadPool();
        ExecutorService poolForOutput = Executors.newCachedThreadPool();

        System.out.println("\nManager Run:");
        // Loop Split variables
        String inputFileName;
        int numberOfLinesInTheLocalAppFile;
        String summeryFilesIndicatorQueue ;
        String terminationIndicator;
        List<Message> currMessageQueue;
        String[] messageContent;
        Message currMessege;
        while (continueRunning.get()) {
            /*if (numberOfReceivedtasksFromTotalOfLocals.get() == numberOfCompletedTasks.get()) {
            System.out.println("Manager numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals.get());
            System.out.println("Manager number Of Tasks sent to workers are: " + numberOfTasks.get());
            System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + numberOfCompletedTasks.get());
            }*/

            // check if more workers are needed
            createworker(ec2,numberOfTasks.get());
            // Recieve message from local app queue
            currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
            if (currMessageQueue != null && !currMessageQueue.isEmpty()) {

                // Split the message from the LocalApp
                currMessege = currMessageQueue.get(0);
                messageContent = currMessege.getBody().split("@");
                inputFileName = messageContent[0];
                numberOfLinesInTheLocalAppFile = Integer.parseInt(messageContent[1]);
                summeryFilesIndicatorQueue = messageContent[2];
                terminationIndicator = messageContent[3];

                // Print process input message split results
                System.out.println();
                System.out.println("Input From Local App processing split results:");
                System.out.println("inputFileName: " + inputFileName + "numberOfLinesInTheLocalAppFile: " + numberOfLinesInTheLocalAppFile);
                System.out.println("summeryFilesIndicatorQueue: " + summeryFilesIndicatorQueue + "terminationIndicator: " + terminationIndicator);

                //Print input message process results
                numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(numberOfReceivedtasksFromTotalOfLocals.get() + numberOfLinesInTheLocalAppFile);
                String inputFileID  = UUID.randomUUID().toString();
                continueRunning.set(!terminationIndicator.equals("terminate"));
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                queue.deleteMessage(QueueUrlLocalApps, currMessege);

                // Create input file object
                InputFileObject newFile = new InputFileObject(inputFileName, numberOfLinesInTheLocalAppFile, reader, inputFileID, summeryFilesIndicatorQueue);
                InputFileObjectById.putIfAbsent(inputFileID, newFile);

                // Create Completed tasks queue unique for the input file object
                queue.createQueue(inputFileID);

                // calculate number of threads to open
                int numberOfThreadsToLaunch = (numberOfLinesInTheLocalAppFile / 50) + 1;
                System.out.println("\nNumber of threads to launch for the input file: " + inputFileName + " are: " + numberOfThreadsToLaunch + "\n");

                // open input and output threads for a file from local app
                for (int i = 0; i < numberOfThreadsToLaunch; ++i) {
                    poolForInput.execute(new InputThread(newFile, numberOfTasks));
                    poolForOutput.execute(new OutputThread(newFile, numberOfCompletedTasks));
                }
            }
            
            System.out.println("\nGoing over the upload loop");
            boolean inputHasFinished = false;
            System.out.println("\"\n");
            System.out.println("\nlist size: " + InputFileObjectById.size());                   
            for (InputFileObject currFileObject :
                    InputFileObjectById.values()) {
                if (currFileObject != null ){
                    synchronized (currFileObject){
                        inputHasFinished = currFileObject.getAllWorkersDone();
                    }
                }
                if (inputHasFinished){
                    synchronized (currFileObject) {
                        System.out.println("Input fIle object details: ");
                        System.out.println(currFileObject);
                        String outputName = currFileObject.getInputFilename() + "$";
                        String getSummeryFilesIndicatorQueue = currFileObject.getSummeryFilesIndicatorQueue();
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + outputName));
                        bufferedWriter.write(currFileObject.getBuffer().toString());
                        bufferedWriter.flush();
                        queue.sendMessage(getSummeryFilesIndicatorQueue, outputName);
                        InputFileObjectById.remove(currFileObject.getInputFileID(),currFileObject);
                        queue.deleteQueue(currFileObject.getInputFileID(),"");
                        s3.upload(path, outputName);
                    }
                }
            }
        }
        poolForInput.shutdown();
        poolForOutput.shutdown();

        // at this point all threads finished working due to a termination message, meaning all client we committed to serve received an answer
        // we need to clean all resources the LocalApp queue
        System.out.println("\n Manager termiante deleting resources\n");
        queue.deleteQueue("QueueUrlLocalApps","");
        queue.deleteQueue("workerJobQueue","");
        ec2.terminateInstances(null);
    }

    public static void createworker(EC2Object ec2, int numberOfTasks){

        int workerinstances = ec2.getInstances("worker").size();

        if ( !(numberOfTasks % 200==0) || workerinstances > 14 || workerinstances*200 > numberOfTasks){
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
        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");
    }
}
