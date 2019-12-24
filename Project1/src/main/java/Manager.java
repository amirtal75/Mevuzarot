import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager{

    public static void main(String[] args) throws Exception {
        String QueueUrlLocalApps = "QueueUrlLocalApps";
        String summeryFilesIndicatorQueue = "summeryFilesIndicatorQueue";
        String workerJobQueue = "workerJobQueue";
        String completedTasksQueue = "completedTasksQueue";
        System.out.println("In Manager:");
        S3Bucket s3 = new S3Bucket();
        EC2Object ec2 = new EC2Object();

        Queue queue = new Queue();
        System.out.println();
        System.out.println("Worker Receiving Queue: " + workerJobQueue + ", Task Results Queue: " + completedTasksQueue);

        createworker(workerJobQueue,completedTasksQueue, ec2, queue,0);
        System.out.println("Created the first worker");

        AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicBoolean continueRunning = new AtomicBoolean(true);
        ArrayList<String> reviewIDList = new ArrayList<>();
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<String, InputFileObject> InputFileObjectById = new ConcurrentHashMap<>();

        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

        List<Message> currMessageQueue = null;
        // calculate number of threads to open

        // Create Thread Pools
        boolean run = true;
        while (continueRunning.get()) {
            if (numberOfReceivedtasksFromTotalOfLocals.get() == numberOfCompletedTasks.get() && numberOfReceivedtasksFromTotalOfLocals.get() > 0) {
                System.out.println("Manager numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals.get());
                System.out.println("Manager number Of Tasks sent to workers are: " + numberOfTasks.get());
                System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + numberOfCompletedTasks.get());
            }



            // Recieve message from local app queue
            currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
            String[] messageContent;

            if (!currMessageQueue.isEmpty()){
                Message currMessege = currMessageQueue.get(0);
                messageContent = currMessege.getBody().split("@");
                int numberOfLinesInTheLocalAppFile = Integer.parseInt(messageContent[1]);
                numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(numberOfReceivedtasksFromTotalOfLocals.get() + numberOfLinesInTheLocalAppFile);
                System.out.println("\n\n\n\n\nDownloading an object with key: " + messageContent[0] + "\n\n\n\n\n\n\n");
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                // queue.deleteMessage(QueueUrlLocalApps,currMessege);
                queue.purgeQueue(workerJobQueue);

                // check termination condirion
                if (messageContent.length > 2){
                    run = (numberOfReceivedtasksFromTotalOfLocals == numberOfCompletedTasks);
                    continueRunning.set(run);
                }

                // Create input file object
                InputFileObject newFile = new InputFileObject(messageContent[0], Integer.parseInt(messageContent[1]), object);
                reviewIDList.add(newFile.getInputFileID());
                InputFileObjectById.putIfAbsent(newFile.getInputFileID(), newFile);

                // Create Completed tasks queue unique for the input file object
                queue.createQueue(newFile.getInputFileID(),true);

                // calaculate threads to launch
                int numberOfThreadsToLaunch = (newFile.getNumberoffilelines() / 50) + 1;
                // System.out.println("Number of input threads to launch is: " +numberOfThreadsToLaunch);

                // open input and output threads for a file from local app
                for (int i = 0; i < numberOfThreadsToLaunch; ++i ){
                    System.out.println("Manager: id of input file: " + newFile.getInputFileID());
                    new Thread(new InputThread(newFile, numberOfTasks)).start();
                    new Thread(new OutputThread(newFile, numberOfCompletedTasks)).start();
                }
            }

            InputFileObject currFileObject = null;
            // System.out.println("InputFileObjectById.size(): " + InputFileObjectById.size());
            for (int i = 0 ; i < InputFileObjectById.size(); ++i){
                String id = reviewIDList.get(i);
                currFileObject = InputFileObjectById.get(id);
                if (currFileObject != null && currFileObject.getAllWorkersDone().get()){
                    try {
                        String outputName = currFileObject.getInputFilename() + "$";
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + outputName));
                        bufferedWriter.write(currFileObject.getBuffer().toString());
                        bufferedWriter.flush();
                        s3.upload(path, outputName);
                        queue.sendMessage(summeryFilesIndicatorQueue, outputName);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }

        }
    }

    private static void createworker(String workerJobQueue, String completedTasksQueue, EC2Object ec2, Queue queue, int numberOfTasks){

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
