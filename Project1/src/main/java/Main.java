import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import edu.stanford.nlp.ling.tokensregex.types.Tags;

import java.util.List;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws Exception {

        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        // !!!!!!!!!!!!!! need to delete !!!!!!!!!!!!
        System.out.println("!!!!!!!!!!!!!terminating!!!!!!!!!!!!!!!!!");
        ec2.terminateInstances(null);
        System.out.println("!!!!!!!!!!!!!purging!!!!!!!!!!!!!!!!!");
        queue.purgeQueue("QueueUrlLocalApps");
        queue.purgeQueue("summeryFilesIndicatorQueueUrl");

        System.out.println("!!!!!!!!!!!!!Creating!!!!!!!!!!!!!!!!!");
        String QueueUrlLocalApps = queue.createQueue("QueueUrlLocalApps");
        String summeryFilesIndicatorQueue = queue.createQueue("summeryFilesIndicatorQueue"+ UUID.randomUUID().toString());
        String workerJobQueue = queue.createQueue("workerJobQueue"); //queue for inputTask for workers
        String completedTasksQueue = queue.createQueue("completedTasksQueue");//queue for outputTask from workers

        System.out.println("!!!!!!!!!!!!!Instantiating!!!!!!!!!!!!!!!!!");
        createManager(QueueUrlLocalApps, summeryFilesIndicatorQueue);
        createWorker(workerJobQueue, completedTasksQueue);

        LocalApp localApp = new LocalApp("inputFile1.txt", QueueUrlLocalApps, summeryFilesIndicatorQueue);
        Thread app = new Thread(localApp);
        app.start();
        new Thread(new LocalApp("inputFile2.txt", QueueUrlLocalApps, summeryFilesIndicatorQueue)).start();



        // ssh instructions
        // open new terminal window
        //cd Downloads
        // ssh -i "projectKey.pem" ubuntu@ssh -i "projectKey.pem" root@ec2-34-214-234-234.us-west-2.compute.amazonaws.com
        // write yes and enter
        // check instance log command = cat /var/log/cloud-init-output.log
//
    }

    private static void createManager(String send, String recieve){
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
            String pushFirstArg = createManagerArgsFile + "echo " + send + " >> src/main/java/managerArgs.txt\n";
            String filedata = pushFirstArg + "echo " + recieve + " >> src/main/java/managerArgs.txt\n";
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
    private static void createWorker(String workerJobQueue, String completedTasksQueue){
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
        String pushFirstArg =  createWorkerArgsFile + "echo " + workerJobQueue + " >> src/main/java/workerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + completedTasksQueue + " >> src/main/java/workerArgs.txt\n";

        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + filedata + createAndRunProject;
        System.out.println("In LocalAPP: " + Thread.currentThread());
        System.out.println("workerJobQueue: " + workerJobQueue + ", completedTasksQueue: " + completedTasksQueue);

        // First created instance = worker
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "worker");

    }
}

//ssh -i "projectKey.pem" ubuntu@ec2-34-217-214-240.us-west-2.compute.amazonaws.com
