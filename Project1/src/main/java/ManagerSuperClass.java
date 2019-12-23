import com.amazonaws.services.ec2.model.Instance;

public class ManagerSuperClass {
    protected static String QueueUrlLocalApps = "https://sqs.us-west-2.amazonaws.com/002041186709/QueueUrlLocalApps";
    protected static String summeryFilesIndicatorQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/summeryFilesIndicatorQueue";
    protected static String workerJobQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/workerJobQueue";
    protected static String completedTasksQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/completedTasksQueue";

    public synchronized static void createworker(String myQueueUrl1, String myQueueUrl2, int numberOfTasks, EC2Object ec2){

        int workerinstances = ec2.getInstances("").size() - 1;
        Boolean tasksDivides = (numberOfTasks % 80) == 0;
        int tasks = numberOfTasks/80;
        Boolean condition = tasksDivides == false && workerinstances < (tasks);

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

        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg = createWorkerArgsFile + "echo " + myQueueUrl1 + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + myQueueUrl2 + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");

    }
}
