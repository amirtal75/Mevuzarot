import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main1 {

    public static void main(String[] args) throws Exception {
        EC2Object ec2Object = new EC2Object();
        for (int i = 0; i < 15; i = i+1){
            createworker(ec2Object,10000);
        }

        int MB = 1024*1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        //Print used memory
        System.out.println("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / MB);

        //Print free memory
        System.out.println("Free Memory:"
                + runtime.freeMemory() / MB);

        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / MB);

        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / MB);

    }

    public static void createworker(EC2Object ec2, int numberOfTasks){

        int workerinstances = ec2.getInstances("worker").size();

        if ( workerinstances > 14 || workerinstances*200 > numberOfTasks){
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

    private static Instance createManager(Queue queue, EC2Object ec2){

        System.out.println("No Manager Active, setting up the server");
        if (!ec2.getInstances("manager").isEmpty()){
            return null;
        }

        // Manager userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + createAndRunProject;

        // First created instance = manager
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("manager",instance.getInstanceId());
        ec2.attachTags(instance, "manager");

        queue.createQueue("QueueUrlLocalApps");
        queue.createQueue("workerJobQueue");
        System.out.println("Creating Manager: " + instance.getInstanceId());
        createworker(ec2,100);
        return  instance;
    }
}




 /*System.out.println(ec2Object.getInstances("manager").size());
        System.out.println(ec2Object.getInstances("worker").size());*/

//ec2Object.terminateInstances(null);

        /*String pathtoPtojectLocation = args[0];
        for (int i = 1; i < args.length; i++){
            System.out.println(pathtoPtojectLocation+ "/" + args[i]);
        }*/

        /*String QueueUrlLocalApps = "QueueUrlLocalApps";
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
        queue.sendMessage(QueueUrlLocalApps, "inputFile2.txtec39dcfd-181e-4e23-b2c2-6fca97cca18e.txt" + "@" + 30 + "@" + "summery" + "@" + "dont close the manager");
        queue.sendMessage(QueueUrlLocalApps, "inputFile1.txt353ddf90-33eb-4795-904e-fb0fa0597956.txt" + "@" + 30 + "@" + "summery" + "@" + "dont close the manager");


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

            Manager.MethodTest(s3, queue, path, InputFileObjectById);
        }
        poolForInput.shutdown();
        poolForOutput.shutdown();
        Thread.sleep(2000);*/

