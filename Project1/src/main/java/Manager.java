import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager{


    public static void main(String[] args) throws Exception {
        System.out.println("In Manager:");

        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        String myQueueUrl1 = queue.createQueue(); //queue for inputTask for workers
        String myQueueUrl2 = queue.createQueue();//queue for outputTask from workers
        System.out.println("Worker Receiving Queue: " + myQueueUrl1 + ", Task Results Queue: " + myQueueUrl2);


        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);

        createworker(myQueueUrl1,myQueueUrl2, numberOfTasks);
        System.out.println("Created the first worker");

        S3Bucket s3 = new S3Bucket();
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        Instance instance = null;
        ConcurrentHashMap<Integer, StringBuffer> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        boolean shouldTerminate = false;
        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer, InputFileObject>();

        // Read the Queue names from the managerArgs file
        try{
            reader = new BufferedReader(new FileReader("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/managerArgs.txt"));

            QueueUrlLocalApps = reader.readLine();
            summeryFilesIndicatorQueue = reader.readLine();
        } catch (IOException e){
            System.out.println(e.getMessage());
        }

        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

        // Create Thread Pools
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;

        while (!shouldTerminate) {

            System.out.println(" Manager :numberOfTasks: " + numberOfTasks.get());
            System.out.println(" Manager :numberOfCompletedTasks: " + numberOfCompletedTasks.get());

            // create worker if needed
            createworker(myQueueUrl1, myQueueUrl2, numberOfTasks);

            try {

                // Recieve message from local app queue
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
                if (currMessageQueue.size() > 0){
                    Message currMessege = currMessageQueue.get(0);
                    String messageContent = currMessege.getBody();
                    System.out.println("Name of file from local app:" + messageContent);

                    //String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuffer> stringResultsById, String QueueUrlLocalApps
                    poolForInput.execute(new InputThread(QueueUrlLocalApps, myQueueUrl1, myQueueUrl2,InputFileObjectById, messageContent, numberOfTasks));

                    // Might need to add future
                    poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById,stringResultsById,  summeryFilesIndicatorQueue,numberOfCompletedTasks));
                    poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, stringResultsById, summeryFilesIndicatorQueue,numberOfCompletedTasks));

                    //System.out.println("Received result from input thread, we need to delete the message");
                    queue.deleteMessage(QueueUrlLocalApps, currMessege); // result = currMessag
                    }

                else{
                    Thread.sleep(3000);
                }

                // Chechk for done files
                for (InputFileObject currInputFileObj : InputFileObjectById.values()) {
                    System.out.println("Upload loop check:\n");
                    System.out.println("Input FIle: " + currInputFileObj.getInputFilename() + "input: " + currInputFileObj.getInputLines() + "output: " + currInputFileObj.getOutputLines() + "done: " + currInputFileObj.getAllWorkersDone());
                    currInputFileObj.CheckAndSetAllWorkersDone();
                    System.out.println("manager : checking if the file " + currInputFileObj.getInputFilename() + " is ready:" + currInputFileObj.getAllWorkersDone());
                    if (currInputFileObj.getAllWorkersDone().get()) {// if all workers done
                        System.out.println("in done loop");
                        FileOutputStream outputFile = null;
                        try {
                            String outputName = currInputFileObj.getInputFilename() + "$";
                            //added "$" to the name because I dont want exact names for the input file and output file
                            Writer writer = new BufferedWriter(new FileWriter(path + outputName)); //write to the output file
                            //System.out.println("\n\n\nStringbuilder contents: \n\n\n");
                            writer.write(stringResultsById.get(currInputFileObj.getId()).toString());
                            writer.flush();

                            System.out.println("preparing to upload the completed file");
                            s3.upload(path, outputName);
                            queue.sendMessage(summeryFilesIndicatorQueue, outputName); // outputFilename = key ??????
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }

                poolForInput.shutdown();
                poolForOutput.shutdown();

            } catch (Exception e){
                //System.out.println(e.getMessage());
            }
        }
    }

    public static void createworker(String myQueueUrl1, String myQueueUrl2, AtomicInteger numberOfTasks){

        EC2Object ec2 = new EC2Object();
        int instanceSize = ec2. getInstances("").size();
        if (numberOfTasks.get() % 80 != 0 && instanceSize-1 > numberOfTasks.get() / 80){
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

