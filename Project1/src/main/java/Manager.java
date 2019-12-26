import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
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
        ArrayList<String> inputNamesReceived =  new ArrayList<>();
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
                if (continueRunning.get() == false){
                    poolForInput.shutdown();
                    poolForOutput.shutdown();
                }
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                queue.deleteMessage(QueueUrlLocalApps, currMessege);

                // Create Completed tasks queue unique for the input file object
                queue.createQueue(inputFileID);

                // check if more workers are needed
                int numberOfWorkersNeededForFile = numberOfLinesInTheLocalAppFile/100;
                int numberOfActiveWorkers = ec2.getInstances("worker").size();
                while (numberOfActiveWorkers < numberOfWorkersNeededForFile){
                    createworker(ec2,numberOfTasks.get());
                    ++numberOfActiveWorkers;
                }


                // calculate number of threads to open
                int numberOfThreadsToLaunch = (numberOfLinesInTheLocalAppFile / 150) + 1;
                System.out.println("\nNumber of threads to launch for the input file: " + inputFileName + " are: " + numberOfThreadsToLaunch + "\n");

                // Create input file object
                if (!inputNamesReceived.contains(inputFileName)) {
                    InputFileObject newFile = new InputFileObject(inputFileName, numberOfLinesInTheLocalAppFile, reader, inputFileID, summeryFilesIndicatorQueue);
                    InputFileObjectById.put(inputFileID, newFile);
                    inputNamesReceived.add(inputFileName);

                    // open input and output threads for a file from local app
                    for (int i = 0; i < numberOfThreadsToLaunch; ++i) {
                        poolForInput.execute(new InputThread(newFile, numberOfTasks));
                        poolForOutput.execute(new OutputThread(newFile, numberOfCompletedTasks));
                    }
                }
            }

            boolean inputHasFinished = false;
            for (InputFileObject currFileObject :
                    InputFileObjectById.values()) {
                if (currFileObject != null ){
                    synchronized (currFileObject){
                        inputHasFinished = currFileObject.getAllWorkersDone();
                    }
                }
                if (inputHasFinished){
                    synchronized (currFileObject) {
                        String outputName = currFileObject.getInputFilename() + "$";
                        String getSummeryFilesIndicatorQueue = currFileObject.getSummeryFilesIndicatorQueue();
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + outputName));
                        bufferedWriter.write(currFileObject.getBuffer().toString());
                        bufferedWriter.flush();
                        queue.sendMessage(getSummeryFilesIndicatorQueue, outputName);
                        InputFileObjectById.remove(currFileObject.getInputFileID(),currFileObject);
                        queue.deleteQueue(currFileObject.getInputFileID());
                        s3.upload(path, outputName);
                    }
                }
            }
        }
        // at this point all threads finished working due to a termination message, meaning all client we committed to serve received an answer
        // we need to clean all resources the LocalApp queue
        boolean allThreadsAreDone = false;
        while (!allThreadsAreDone){
            allThreadsAreDone = (numberOfCompletedTasks == numberOfReceivedtasksFromTotalOfLocals) && (numberOfTasks == numberOfReceivedtasksFromTotalOfLocals);
        }
        System.out.println("\n Manager terminated deleting resources\n");
        queue.deleteQueue("QueueUrlLocalApps");
        queue.deleteQueue("workerJobQueue");
        ec2.terminateInstances(null);
    }

    public static void createworker(EC2Object ec2, int numberOfTasks){

        int workerinstances = ec2.getInstances("worker").size();

        if ( workerinstances > 14 || workerinstances*100 > numberOfTasks){
            System.out.println(workerinstances + " " + numberOfTasks);
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