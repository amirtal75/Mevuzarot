import com.amazonaws.services.ec2.model.Instance;

import java.util.List;

public class Main {

    static String summeryFilesIndicatorQueueUrl;
    static String QueueUrlLocalApps;


    public static void main(String[] args) throws Exception {

        EC2Object ec2 = new EC2Object();

        // !!!!!!!!!!!!!! need to delete !!!!!!!!!!!!
        ec2.terminateInstances(null);
        Thread.sleep(1000);

        List<Instance> instances = ec2.getInstances("manager");
        if(instances.isEmpty()){
            createManager(ec2, instances);
            Thread.sleep(1000);
        }

        QueueUrlLocalApps = "https://sqs.us-west-2.amazonaws.com/002041186709/QueueUrlLocalApps";
        summeryFilesIndicatorQueueUrl = "https://sqs.us-west-2.amazonaws.com/002041186709/summeryFilesIndicatorQueueUrl";


        LocalApp localApp = new LocalApp("inputFile1.txt", QueueUrlLocalApps, summeryFilesIndicatorQueueUrl);
        Thread app = new Thread(localApp);
        app.start();
        new Thread(new LocalApp("inputFile2.txt", QueueUrlLocalApps, summeryFilesIndicatorQueueUrl)).start();



        // ssh instructions
        // open new terminal window
        //cd Downloads
        // ssh -i "projectKey.pem" ubuntu@ssh -i "projectKey.pem" root@ec2-34-214-234-234.us-west-2.compute.amazonaws.com
        // write yes and enter
        // check instance log command = cat /var/log/cloud-init-output.log
//
    }

    private static void createManager(EC2Object ec2, List<Instance> instances){

        System.out.println("No Manager Active, setting up the server");

        // Manager userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "sudo touch src/main/java/managerArgs.txt\n";
        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueueUrl + " >> src/main/java/managerArgs.txt\n";

        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + filedata + createAndRunProject;
        System.out.println("In LocalAPP: " + Thread.currentThread());
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueueUrl);

        // First created instance = manager
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        System.out.println("created the manger: " + instance.getInstanceId());

        ec2.attachTags(instance, "manager");

        Queue queue = new Queue();
        String QueueUrlLocalApps = queue.createQueue("QueueUrlLocalApps");
        String summeryFilesIndicatorQueueUrl = queue.createQueue("summeryFilesIndicatorQueueUrl");

    }
}