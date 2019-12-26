import com.amazonaws.services.ec2.model.Instance;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static String summeryFilesIndicatorQueueUrl;
    static String QueueUrlLocalApps;


    public static
    void main(String[] args) throws Exception {

        String pathtoPtojectLocation = args[0];
        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();

        // Create bucket
        new S3Bucket().createBucket();
        // Create manager and worker if not already opened
        Instance instance = createManager(queue, ec2);

        Thread thread = null;
        LocalApp localApp = null;
        int terminationIndicator = 0;
        if (args[args.length-1].equals("terminate")){
            terminationIndicator = 1;
        }
        for (int i = 1; i < args.length - 1 + terminationIndicator; i++) {
            localApp = new LocalApp(pathtoPtojectLocation, args[i], args[args.length - 1], ec2);
            thread = new Thread(localApp);
            thread.start();
        }
    }


    private static void createWorker(EC2Object ec2){

        if (!ec2.getInstances("worker").isEmpty()){
            return;
        }

        // worker userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject  + createAndRunProject;

        // First created instance = worker
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "worker");
        System.out.println("Creating Worker: " + instance.getInstanceId());


    }

    private static Instance createManager(Queue queue, EC2Object ec2){


        if (!ec2.getInstances("manager").isEmpty()){
            return new Instance();
        }
        System.out.println("No Manager Active, setting up the server");
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

        System.out.println("creating: " +queue.createQueue("QueueUrlLocalApps"));
        System.out.println("creating: " + queue.createQueue("workerJobQueue"));
        System.out.println("Creating Manager: " + instance.getInstanceId());
        createWorker(ec2);
        return  instance;
    }
}