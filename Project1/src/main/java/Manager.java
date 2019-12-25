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

public class Manager {


    public static void main(String[] args) throws Exception {
        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<Integer, StringBuffer> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        // Read the Queue names from the managerArgs file
        try{
            reader = new BufferedReader(new FileReader("/home/ubuntu/Mevuzarot-master/Project1/src/main/java/managerArgs.txt"));

            QueueUrlLocalApps = reader.readLine();
            summeryFilesIndicatorQueue = reader.readLine();
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println();
        System.out.println("In Manager:");
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

        // Variables Creation
        boolean shouldTerminate = false;
        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer, InputFileObject>();
        ArrayList<InputFileObject> InputFileObjectList = new ArrayList<InputFileObject>();//????
//        ConcurrentHashMap<Integer, StringBuffer> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        Queue queue = new Queue();
        S3Bucket s3 = new S3Bucket();
        EC2Object ec2 = new EC2Object();

        // Create Queues
        String workerJobQueue = queue.createQueue("workerJobQueue"); //queue for inputTask for workers
        String completedTasksQueue = queue.createQueue("completedTasksQueue");//queue for outputTask from workers
        System.out.println("Worker Receiving Queue: " + workerJobQueue + ", Task Results Queue: " + completedTasksQueue);

        // Create Thread Pools
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;
        AtomicInteger workersload = new AtomicInteger(0);
        AtomicInteger messageBytesReceived  = new AtomicInteger(0);
        while (!shouldTerminate) {
            if (numberOfCompletedTasks.get() % 30 == 0) {
            System.out.println("\n\nManager numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals.get());
            System.out.println("Manager number Of Tasks sent to workers are: " + numberOfTasks.get());
            System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + numberOfCompletedTasks.get()+ "\n\n");
            }

            //check if more workers are needed
            createworker(workerJobQueue,completedTasksQueue,ec2,queue,numberOfTasks.get());

            try {
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
                if (currMessageQueue != null && !currMessageQueue.isEmpty()){

                    Message currMessege = currMessageQueue.get(0);
                    String messageContent = currMessege.getBody();
                    String inputFilename = currMessege.getBody();

                    S3Object object = s3.downloadObject(inputFilename); //input file
                    BufferedReader inputFileFromLocalApp = new BufferedReader(new InputStreamReader(object.getObjectContent()));

                    //String completedTasksQueue, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuilder> stringResultsById, String QueueUrlLocalApps
                    poolForInput.execute(new InputThread(workerJobQueue, completedTasksQueue, InputFileObjectById, messageContent, numberOfTasks, inputFileFromLocalApp));
                    poolForInput.execute(new InputThread(workerJobQueue, completedTasksQueue, InputFileObjectById, messageContent, numberOfTasks, inputFileFromLocalApp));
                    // Might need to add future
                    poolForOutput.execute(new OutputThread(completedTasksQueue, InputFileObjectById,stringResultsById,  summeryFilesIndicatorQueue,numberOfCompletedTasks));
                    poolForOutput.execute(new OutputThread(completedTasksQueue, InputFileObjectById, stringResultsById, summeryFilesIndicatorQueue,numberOfCompletedTasks));

                    // processed the message and now we delete
                    queue.deleteMessage(QueueUrlLocalApps, currMessege); // result = currMessag
                }
            } catch (Exception e){
                e.printStackTrace();
            }
            InputFileObject currInputFileObj;
            for (int i = 1; i< InputFileObjectById.size(); ++i) {
                if (InputFileObjectById.containsKey(i)){
                    currInputFileObj = InputFileObjectById.get(i);
                    currInputFileObj.CheckAndSetAllWorkersDone();
                    //System.out.println("manager : checking if the file " + currInputFileObj.getInputFilename() + " is ready:" + currInputFileObj.getAllWorkersDone());
                    System.out.println("\nnInputFile details:\n " + currInputFileObj+ "\n\n");
                    if (currInputFileObj.getAllWorkersDone().get()) {// if all workers done

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


            }
        }

        poolForInput.shutdown();
        poolForOutput.shutdown();
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

