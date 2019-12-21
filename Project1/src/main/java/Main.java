
public class Main {




    public static void main(String[] args) throws Exception {

        Queue queue = new Queue();
        //String summeryFilesIndicatorQueue = queue.createQueue();
        //String QueueUrlLocalApps = queue.createQueue();
        EC2Object ec2 = new EC2Object();
        // how to check user data inside putty connection
        // cd var/lib/cloud/instance
        // sudo cat user-data.txt
        /*String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn -T 4 install\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + filedata +createAndRunProject;
        System.out.println("In LocalAPP: " + Thread.currentThread());
        System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);
        System.out.println("UserData: " + userdata);*/
        ec2.terminateInstances(null);
        //Instance createInstance = ec2.createInstance(1,1,userdata).get(0);
        //System.out.println(ec2.getInstances("manager").get(0).getInstanceId());;
        LocalApp localApp = new LocalApp("inputFile1.txt");
        Thread app = new Thread(localApp);
        app.start();

        // ssh instructions
        // open new terminal window
        //cd Downloads
        // ssh -i "projectKey.pem" ubuntu@ssh -i "projectKey.pem" root@ec2-34-214-234-234.us-west-2.compute.amazonaws.com
        // write yes and enter
        // check instance log command = cat /var/log/cloud-init-output.log
//
    }
}

//ssh -i "projectKey.pem" ubuntu@ec2-34-217-214-240.us-west-2.compute.amazonaws.com
