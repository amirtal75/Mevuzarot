import com.amazonaws.services.ec2.model.Instance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagerSuperClass {
    public  String QueueUrlLocalApps = "QueueUrlLocalApps";
    public  String summeryFilesIndicatorQueue = "summeryFilesIndicatorQueue";
    public  String workerJobQueue = "workerJobQueue";
    public  String completedTasksQueue = "completedTasksQueue";
    public  AtomicInteger numberOfReceivedtasksFromTotalOfLocals = new AtomicInteger(0);
    public  AtomicInteger numberOfTasks = new AtomicInteger(0);
    public  AtomicInteger numberOfCompletedTasks = new AtomicInteger(0);
    public  AtomicInteger idOfInputFile = new AtomicInteger(0);
    protected AtomicBoolean continueRunning = new AtomicBoolean(true);

    public
    ManagerSuperClass() {
    }
    public
    ManagerSuperClass(ManagerSuperClass manager) {
        numberOfCompletedTasks = manager.numberOfCompletedTasks;
        numberOfTasks=manager.numberOfTasks;
        numberOfReceivedtasksFromTotalOfLocals=manager.numberOfReceivedtasksFromTotalOfLocals;
        idOfInputFile = manager.idOfInputFile;
        continueRunning = manager.continueRunning;
    }
    public synchronized
    int getNumberOfReceivedtasksFromTotalOfLocals() {
        return numberOfReceivedtasksFromTotalOfLocals.get();
    }

    public synchronized
    void setNumberOfReceivedtasksFromTotalOfLocals(int numberOfReceivedtasksFromTotalOfLocals) {
        this.numberOfReceivedtasksFromTotalOfLocals.set(numberOfReceivedtasksFromTotalOfLocals);
    }

    public synchronized
    int getNumberOfTasks() {
        return numberOfTasks.get();
    }

    public synchronized
    void setNumberOfTasks(int numberOfTasks) {
        this.numberOfTasks.set(numberOfTasks);
    }

    public synchronized
    int getNumberOfCompletedTasks() {
        return numberOfCompletedTasks.get();
    }

    public synchronized
    void setNumberOfCompletedTasks(int numberOfCompletedTasks) {
        this.numberOfCompletedTasks.set(numberOfCompletedTasks);
    }

    public synchronized
    int getIdOfInputFile() {
        return idOfInputFile.get();
    }

    public synchronized
    void setIdOfInputFile(int idOfInputFile) {
        this.idOfInputFile.set(idOfInputFile);
    }

    public synchronized
    boolean getContinueRunning() {
        return continueRunning.get();
    }

    public synchronized
    void setContinueRunning(boolean continueRunning) {
        this.continueRunning.set(continueRunning);
    }

    public void createworker(EC2Object ec2, Queue queue){

        int workerinstances = ec2.getInstances("").size() - 1;
        Boolean tasksDivides = (getNumberOfTasks() % 80) == 0;
        int tasks = getNumberOfTasks()/80;
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

        String createWorkerArgsFile = "touch src/main/java/workerArgs.txt\n";
        String pushFirstArg = createWorkerArgsFile + "echo " + workerJobQueue + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + completedTasksQueue + " >> src/main/java/workerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;

        Instance instance = ec2.createInstance(1, 1, workerUserData).get(0);
        ec2.attachTags(instance, "worker");
        System.out.println("created new worker instance: " + instance.getInstanceId() + "\n\n\n\n");

    }
}
