import com.amazonaws.services.ec2.model.Instance;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Main1 {

    public static void main(String[] args) throws IOException {
        Queue queue = new Queue();
        String QueueUrlLocalApps = ("QueueUrlLocalApps");
        String summeryFilesIndicatorQueue = ("summeryFilesIndicatorQueue"+ UUID.randomUUID().toString());
        String workerJobQueue = ("workerJobQueue"); //queue for inputTask for workers
        String completedTasksQueue = ("completedTasksQueue");//queue for outputTask from workers
        queue.sendMessage(QueueUrlLocalApps, "inputFile1.txte6bc03d0-35ed-40e8-ab02-6f01b2423304.txt");
        queue.sendMessage(QueueUrlLocalApps, "inputFile2.txt3ce68107-9734-45ed-9f2f-e4b708533aef.txt");

        //createWorker(QueueUrlLocalApps, summeryFilesIndicatorQueue);
    }
    private synchronized static void createManager(String send, String recieve){
        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        System.out.println(ec2.getInstances("manager").isEmpty());
        if (ec2.getInstances("manager").isEmpty()) {

            System.out.println("Creating manager from local app");

            // Manager userdata
            String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
            String unzip = getProject + "sudo unzip -o master.zip\n";
            String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
            String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
            String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
            String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
            String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

            String createManagerArgsFile = "sudo touch src/main/java/managerArgs.txt\n";
            String pushFirstArg = createManagerArgsFile + "echo " + recieve + " >> src/main/java/managerArgs.txt\n";
            String filedata = pushFirstArg + "echo " + send + " >> src/main/java/managerArgs.txt\n";
            String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata;
            //String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" + buildProject + filedata + createAndRunProject;
            System.out.println("In LocalAPP: " + Thread.currentThread());
            System.out.println("Local Queue: " + send + ", Summary Queue: " + recieve);

            // First created instance = manager
            Instance instance = ec2.createInstance(1, 1, userdata).get(0);
            ec2.createTags("manager",instance.getInstanceId());
            ec2.attachTags(instance, "manager");
        }
    }
    private synchronized static void createWorker(String send, String recieve){
        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        System.out.println("Creating worker from local app");

        // worker userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

        String createWorkerArgsFile = "sudo touch src/main/java/workerArgs.txt\n";
        String pushFirstArg =  createWorkerArgsFile + "echo " + recieve + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + send + " >> src/main/java/workerArgs.txt\n";

        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + filedata + createAndRunProject;
        System.out.println("In LocalAPP: " + Thread.currentThread());
        System.out.println("Local Queue: " + send + ", Summary Queue: " + recieve);

        // First created instance = worker
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "worker");

    }
}
