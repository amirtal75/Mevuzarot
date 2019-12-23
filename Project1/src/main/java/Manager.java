import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
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

        int numberOfReceivedtasksFromTotalOfLocals = 0;
        System.out.println("In Manager:");

        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        String myQueueUrl1 = queue.createQueue(); //queue for inputTask for workers
        String myQueueUrl2 = queue.createQueue();//queue for outputTask from workers
        System.out.println("Worker Receiving Queue: " + myQueueUrl1 + ", Task Results Queue: " + myQueueUrl2);


        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicInteger idOfInputFile = new AtomicInteger(0);

        createworker(myQueueUrl1,myQueueUrl2, numberOfTasks.get(), 1);
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
        int numberOfInputThreads = 0;
        int numberOfOutputThreads = 0;
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;

        while (!shouldTerminate) {

            System.out.println(" Manager :numberOfTasks: " + numberOfTasks.get());
            System.out.println(" Manager :numberOfCompletedTasks: " + numberOfCompletedTasks.get());

            try {

                // Recieve message from local app queue
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
                if (currMessageQueue.size() > 0){
                    Message currMessege = currMessageQueue.get(0);
                    String[] messageContent = currMessege.getBody().split("@");
                    numberOfReceivedtasksFromTotalOfLocals += Integer.parseInt(messageContent[1]);

                    System.out.println("\n\n\n\n\nDownloading an object with key: " + messageContent[0] + "\n\n\n\n\n\n\n");
                    S3Object object = s3.downloadObject(messageContent[0]); //input file
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
                    System.out.println("ID before: " + idOfInputFile.get());
                    idOfInputFile.getAndIncrement();
                    System.out.println("ID after: " + idOfInputFile.get());
                    InputFileObject newFile = new InputFileObject(idOfInputFile.get(),messageContent[0],path, Integer.parseInt(messageContent[1]));

                    InputFileObjectById.putIfAbsent(idOfInputFile.get(), newFile); //add the currFileObject with his special id
                    System.out.println("Successfully added a new file object: " + InputFileObjectById.contains(newFile));

                    //String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuffer> stringResultsById, String QueueUrlLocalApps

                    int dividor = (numberOfInputThreads+1) * 100;
                    System.out.println("dividor: " + dividor);
                    int numberOfInputThreadsToLaunch = Math.abs(numberOfReceivedtasksFromTotalOfLocals - numberOfTasks.get()) / dividor;
                    System.out.println("numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals + ", numberOfTasks performed is: " +numberOfTasks.get());
                    System.out.println("Number of input threads to launch is: " +numberOfInputThreadsToLaunch);
                    for (int i = 0; i < numberOfInputThreadsToLaunch; ++i ){
                        System.out.println("Manager: id of input file: " + newFile.getId());
                       new Thread(new InputThread(QueueUrlLocalApps, myQueueUrl1, myQueueUrl2,newFile, bufferedReader, numberOfTasks)).start();
                    }

                    // Might need to add future
                    poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById,stringResultsById,  summeryFilesIndicatorQueue,numberOfCompletedTasks));
                    poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, stringResultsById, summeryFilesIndicatorQueue,numberOfCompletedTasks));

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
            }
        }
    }

    public synchronized static void createworker(String myQueueUrl1, String myQueueUrl2, int numberOfTasks, int instanceSize){
        int workerinstances = instanceSize - 1;
        Boolean tasksDivides = (numberOfTasks % 80) == 0;
        System.out.println("\n\n\n\n\ncreating worker when the current number of instances is: " + workerinstances);
        System.out.println("current number of tasks is: " + numberOfTasks);
        System.out.println("tasksDivides is " + tasksDivides);
        System.out.println("(numberOfTasks / 80) is" + (numberOfTasks / 80));
        EC2Object ec2 = new EC2Object();
        if (instanceSize > 16){
            return;
        }

        if ( tasksDivides == false && workerinstances < (numberOfTasks / 80)){
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

