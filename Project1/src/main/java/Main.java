
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.ec2.model.Instance;

public class Main {




    public static void main(String[] args) throws Exception {

        Queue queue = new Queue();
        String summeryFilesIndicatorQueue = queue.createQueue();
        String QueueUrlLocalApps = queue.createQueue();
        EC2Object ec2 = new EC2Object();

        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot/Project1/\n";
        String buildProject = goToProjectDirectory + "mvn compile\n mvn package\n mvn install\n";
        String createAndRunProject = buildProject + "java -jar  target/maven-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

        String userdata = "#!/bin/bash\n" + createAndRunProject + filedata;
        System.out.println(userdata);
        ec2.terminateInstances(null);
        ec2.createInstance(1,1,userdata);

    }
}
