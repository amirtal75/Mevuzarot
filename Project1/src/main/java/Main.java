
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.simpleworkflow.flow.annotations.Wait;

public class Main {




    public static void main(String[] args) throws Exception {

        Queue queue = new Queue();
        String summeryFilesIndicatorQueue = queue.createQueue();
        String QueueUrlLocalApps = queue.createQueue();
        EC2Object ec2 = new EC2Object();
        // how to check user data inside putty connection
        // cd var/lib/cloud/instance
        // sudo cat user-data.txt
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install\n";
        String createAndRunProject = "sudo java -jar target/core-java-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

        String userdata = "#!/bin/bash\n" +  buildProject + filedata +createAndRunProject;
        System.out.println("In LocalAPP: " + Thread.currentThread());
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);
        System.out.println("UserData: " + userdata);
        ec2.terminateInstances(null);
        ec2.createInstance(1,1,userdata);
        Thread.currentThread().sleep(1000000);
        System.out.println("terminating session");
        ec2.terminateInstances(null);

    }
}
