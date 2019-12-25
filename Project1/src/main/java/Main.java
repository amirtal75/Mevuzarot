import com.amazonaws.services.ec2.model.Instance;

import java.util.List;

public class Main {

    static String summeryFilesIndicatorQueueUrl;
    static String QueueUrlLocalApps;


    public static void main(String[] args) throws Exception {

        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        // !!!!!!!!!!!!!! need to delete following lines !!!!!!!!!!!!
        ec2.terminateInstances(null);
        Thread.sleep(1000);

        LocalApp localApp = new LocalApp("inputFile1.txt");
        Thread app = new Thread(localApp);
        app.start();
        new Thread(new LocalApp("inputFile2.txt")).start();



        // ssh instructions
        // open new terminal window
        //cd Downloads
        // ssh -i "projectKey.pem" ubuntu@ssh -i "projectKey.pem" root@ec2-34-214-234-234.us-west-2.compute.amazonaws.com
        // write yes and enter
        // check instance log command = cat /var/log/cloud-init-output.log
//
    }
}