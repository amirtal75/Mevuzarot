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
        String QueueUrlLocalApps = "QueueUrlLocalApps";
        String workerJobQueue = "workerJobQueue";
        System.out.println("In Manager:");
        S3Bucket s3 = new S3Bucket();
        Queue queue = new Queue();

        AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicBoolean continueRunning = new AtomicBoolean(true);
        ArrayList<String> reviewIDList = new ArrayList<>();
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<String, InputFileObject> InputFileObjectById = new ConcurrentHashMap<>();

        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????
        List<Message> currMessageQueue = null;
        // calculate number of threads to open

        // need to delete
        queue.purgeQueue(QueueUrlLocalApps);
        queue.purgeQueue(workerJobQueue);
        queue.sendMessage(QueueUrlLocalApps, "inputFile1.txte6bc03d0-35ed-40e8-ab02-6f01b2423304.txt@30" + "@" + "summery" + "@" + "dont close the manager");
        queue.sendMessage(QueueUrlLocalApps, "inputFile2.txt3ce68107-9734-45ed-9f2f-e4b708533aef.txt@30" + "@" + "summery" + "@" + "dont close the manager");


        // Create Thread Pools
        boolean run = true;
        while (continueRunning.get()) {
            if (numberOfReceivedtasksFromTotalOfLocals.get() == numberOfCompletedTasks.get()) {
            System.out.println("Manager numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals.get());
            System.out.println("Manager number Of Tasks sent to workers are: " + numberOfTasks.get());
            System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + numberOfCompletedTasks.get());
            }


            // Recieve message from local app queue
            currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
            String[] messageContent;

            if (!currMessageQueue.isEmpty()) {

                Message currMessege = currMessageQueue.get(0);
                messageContent = currMessege.getBody().split("@");
                int numberOfLinesInTheLocalAppFile = Integer.parseInt(messageContent[1]);
                numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(numberOfReceivedtasksFromTotalOfLocals.get() + numberOfLinesInTheLocalAppFile);
                System.out.println("\n\n\n\n\nDownloading an object with key: " + messageContent[0] + "\n\n\n\n\n\n\n");
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                queue.deleteMessage(QueueUrlLocalApps, currMessege);

                // check termination condirion
                if (messageContent.length > 3 && messageContent[3].equals("terminate")) {
                    continueRunning.set(run);
                }

                // Create input file object
                String inputFileID  = UUID.randomUUID().toString();
                String summeryFilesIndicatorQueue = messageContent[2];
                InputFileObject newFile = new InputFileObject(messageContent[0], Integer.parseInt(messageContent[1]), reader, inputFileID, summeryFilesIndicatorQueue);
                reviewIDList.add(inputFileID);
                InputFileObjectById.putIfAbsent(inputFileID, newFile);

                // Create Completed tasks queue unique for the input file object
                queue.createQueue(inputFileID);

                // calaculate threads to launch
                int numberOfThreadsToLaunch = (newFile.getNumberoffilelines() / 50) + 1;
                // System.out.println("Number of input threads to launch is: " +numberOfThreadsToLaunch);

                // open input and output threads for a file from local app
                for (int i = 0; i < numberOfThreadsToLaunch; ++i) {
                    System.out.println("Manager: id of input file: " + newFile.getInputFileID());
                    poolForInput.execute(new InputThread(newFile, numberOfTasks));
                    poolForOutput.execute(new OutputThread(newFile, numberOfCompletedTasks));
                }
            }

            for (InputFileObject currFileObject :
                    InputFileObjectById.values()) {
                if (currFileObject != null && currFileObject.getAllWorkersDone()) {
                    synchronized (currFileObject) {
                        String outputName = currFileObject.getInputFilename() + "$";
                        String getSummeryFilesIndicatorQueue = currFileObject.getSummeryFilesIndicatorQueue();
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + outputName));
                        bufferedWriter.write(currFileObject.getBuffer().toString());
                        queue.sendMessage(getSummeryFilesIndicatorQueue, outputName);
                        InputFileObjectById.remove(currFileObject.getInputFileID(),currFileObject);
                        s3.upload(path, outputName);
                    }

                }
            }
        }
        poolForInput.shutdown();
        poolForOutput.shutdown();
        Thread.sleep(2000);

    }
}
