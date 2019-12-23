import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager extends ManagerClassesSharedFunctions{


    public static void main(String[] args) throws Exception {

        AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
        System.out.println("In Manager:");

        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        String workerJobQueue = queue.createQueue("workerJobQueue"); //queue for inputTask for workers
        String completedTasksQueue = queue.createQueue("completedTasksQueue");//queue for outputTask from workers
        System.out.println("Worker Receiving Queue: " + workerJobQueue + ", Task Results Queue: " + completedTasksQueue);


        AtomicInteger numberOfTasks = new AtomicInteger(0);
        AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
        AtomicInteger idOfInputFile = new AtomicInteger(0);

        createworker(workerJobQueue,completedTasksQueue, numberOfTasks.get(), ec2);
        System.out.println("Created the first worker");

        S3Bucket s3 = new S3Bucket();
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<Integer, StringBuffer> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        BufferedReader reader = null;
        String QueueUrlLocalApps = "";
        String summeryFilesIndicatorQueue = "";
        boolean shouldTerminate = false;

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
        int numberOfThreadsToOpenPerFileFromLocalApp = 0;
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        List<Message> currMessageQueue = null;

        while (!shouldTerminate) {

            System.out.println("Manager numberOfReceivedtasksFromTotalOfLocals is :" + numberOfReceivedtasksFromTotalOfLocals);
            System.out.println("Manager number Of Tasks sent to workers are: " +numberOfTasks.get());
            System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + numberOfCompletedTasks.get());


            // Recieve message from local app queue
            currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility

            if (currMessageQueue.size() > 0){
                Message currMessege = currMessageQueue.get(0);
                String[] messageContent = currMessege.getBody().split("@");
                int numberOfLinesInTheLocalAppFile = Integer.parseInt(messageContent[1]);
                numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(numberOfLinesInTheLocalAppFile + numberOfReceivedtasksFromTotalOfLocals.get());

                System.out.println("\n\n\n\n\nDownloading an object with key: " + messageContent[0] + "\n\n\n\n\n\n\n");
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                idOfInputFile.getAndIncrement();

                // Create input file object
                InputFileObject newFile = new InputFileObject(idOfInputFile.get(),messageContent[0],path, Integer.parseInt(messageContent[1]), object);

                // calculate number of threads to open
                int dividor = (numberOfThreadsToOpenPerFileFromLocalApp + 1) * 100;
                int numberOfThreadsToLaunch = Math.abs(numberOfReceivedtasksFromTotalOfLocals.get() - numberOfTasks.get()) / dividor;
                System.out.println("Number of input threads to launch is: " +numberOfThreadsToLaunch);

                // open input and output threads for a file from local app
                for (int i = 0; i < numberOfThreadsToLaunch; ++i ){
                    System.out.println("Manager: id of input file: " + newFile.getId());
                    new Thread(new InputThread(QueueUrlLocalApps, workerJobQueue, completedTasksQueue,newFile, numberOfTasks)).start();
                    new Thread(new OutputThread(completedTasksQueue, summeryFilesIndicatorQueue, newFile, numberOfCompletedTasks)).start();
                }
            }

            else{
                Thread.sleep(3000);
            }
        }
    }
}
