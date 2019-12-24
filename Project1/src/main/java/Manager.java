import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager extends ManagerSuperClass{

    public static void main(String[] args) throws Exception {
        String QueueUrlLocalApps = "https://sqs.us-west-2.amazonaws.com/002041186709/QueueUrlLocalApps";
        String summeryFilesIndicatorQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/summeryFilesIndicatorQueue";
        String workerJobQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/workerJobQueue";
        String completedTasksQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/completedTasksQueue";
        System.out.println("In Manager:");
        S3Bucket s3 = new S3Bucket();
        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        ManagerSuperClass manager = new ManagerSuperClass();
        System.out.println("Worker Receiving Queue: " + workerJobQueue + ", Task Results Queue: " + completedTasksQueue);

        createworker(workerJobQueue,completedTasksQueue, ec2, queue,0);
        System.out.println("Created the first worker");


        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<>();

        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

        List<Message> currMessageQueue = null;
        // calculate number of threads to open

        // Create Thread Pools
        ExecutorService poolForInput = Executors.newCachedThreadPool(); //Executors.newSingleThreadExecutor(); ??????
        ExecutorService poolForOutput = Executors.newCachedThreadPool(); // Executors.newSingleThreadExecutor();?????

        // Start the upload thread
        new Thread(new UploadThread(InputFileObjectById, manager)).start();

        while (manager.getContinueRunning()) {

            System.out.println("Manager numberOfReceivedtasksFromTotalOfLocals is :" + manager.getNumberOfReceivedtasksFromTotalOfLocals());
            System.out.println("Manager number Of Tasks sent to workers are: " +manager.getNumberOfTasks());
            System.out.println("Manager number Of Tasks received from workers (built into a buffer): " + manager.getNumberOfCompletedTasks());



            // Recieve message from local app queue
            currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 1000); // check about visibility
            String[] messageContent;

            if (!currMessageQueue.isEmpty()){
                Message currMessege = currMessageQueue.get(0);
                messageContent = currMessege.getBody().split("@");
                int numberOfLinesInTheLocalAppFile = Integer.parseInt(messageContent[1]);
                manager.setNumberOfReceivedtasksFromTotalOfLocals(numberOfLinesInTheLocalAppFile + manager.getNumberOfReceivedtasksFromTotalOfLocals());

                System.out.println("\n\n\n\n\nDownloading an object with key: " + messageContent[0] + "\n\n\n\n\n\n\n");
                S3Object object = s3.downloadObject(messageContent[0]); //input file
                manager.setIdOfInputFile(manager.getIdOfInputFile() + 1);

                // Create input file object
                InputFileObject newFile = new InputFileObject(manager.getIdOfInputFile(),messageContent[0],path, Integer.parseInt(messageContent[1]), object);
                InputFileObjectById.put(manager.getIdOfInputFile(), newFile);

                // calaculate threads to launch
                int numberOfThreadsToLaunch = (Math.abs(manager.getNumberOfReceivedtasksFromTotalOfLocals() - manager.getNumberOfTasks()) / 50) + 1;
                System.out.println("Number of input threads to launch is: " +numberOfThreadsToLaunch);

                // open input and output threads for a file from local app
                for (int i = 0; i < numberOfThreadsToLaunch; ++i ){
                    System.out.println("Manager: id of input file: " + newFile.getId());
                    poolForInput.execute(new InputThread(newFile, manager));
                    poolForOutput.execute(new OutputThread(InputFileObjectById, manager));
                }
            }  else{
                Thread.sleep(3000);
            }
        }
    }

    private static void createworker(String workerJobQueue, String completedTasksQueue, EC2Object ec2, Queue queue, int numberOfTasks){

        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg = createWorkerArgsFile + "echo " + workerJobQueue + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + completedTasksQueue + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");

    }
}
