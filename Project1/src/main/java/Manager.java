import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

    public static void main(String[] args) throws Exception {

        // Read the Queue names from the managerArgs file
        BufferedReader reader = new BufferedReader(new FileReader("managerArgs"));
        String QueueUrlLocalApps = reader.readLine();
        String summeryFilesIndicatorQueue = reader.readLine();

        // Variables Creation
        boolean shouldTerminate = false;
        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer, InputFileObject>();
        ArrayList<InputFileObject> InputFileObjectList = new ArrayList<InputFileObject>();//????
        ConcurrentHashMap<Integer, StringBuilder> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        Queue queue = new Queue();
        S3Bucket s3 = new S3Bucket();
        EC2Object ec2 = new EC2Object();

        // create user data dor workers
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip";
        String unzip = getProject + "unzip master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "cp workerpom pom.xml\n";
        String buildProject = setWorkerPom + "mvn compile\n mvn package\n mvn install\n";
        String createAndRunProject = buildProject + "java -jar  target/maven-1.0-SNAPSHOT.jar\n";

        String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
        String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
        String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

        String workerUserData = "#!/bin/bash\n" + createAndRunProject + filedata;
        System.out.println("local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueue);

        String myQueueUrl1 = queue.createQueue(); //queue for inputTask for workers
        String myQueueUrl2 = queue.createQueue();//queue for outputTask from workers

        ExecutorService poolForInput = Executors.newCachedThreadPool();
        ExecutorService poolForOutput = Executors.newCachedThreadPool();

        List<Message> currMessageQueue = null;

        while (!shouldTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(QueueUrlLocalApps, 1, 30); // check about visibility
                }
            catch (Exception e) {
                e.printStackTrace();
            }

            Message currMessege = currMessageQueue.get(0);
            String[] MessageContent = currMessege.getBody().split("@");
            String inputFilename = MessageContent[0];
            String bucketName = MessageContent[1];

            poolForInput.execute(new InputThread(QueueUrlLocalApps, myQueueUrl1, InputFileObjectById, inputFilename, workerUserData));
            poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, stringResultsById, QueueUrlLocalApps));

            queue.deleteMessage(myQueueUrl1, currMessege);
        }

         poolForInput.shutdown();
         poolForOutput.shutdown();
    }
<<<<<<< HEAD


//        List<Message> messages = queue.recieveMessage(QueueUrlLocalApps); //all the locations of the input files from the local-apps
//
//        for (Message message : messages) { // for each location
//            String[] content = message.getBody().split("@");
//            String line;
//            for (int i = 0; i<1 ; i++){
//
//                String key = content[0];
//                String bucketName = content[1];
//                System.out.println("Downloading an object with key: " + key + " from the bucket: " + bucketName);
//                try {
//
//                    S3Object object = s3.downloadObject(key); //input file
//                    upJobIndicatorList.add(new upJobIndicator( key,bucketName, object));
//
//                }
//                catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }

        // adding jobs to inputjobs queue
//        String job;
//        for (upJobIndicator UI:
//                upJobIndicatorList) {
//            job = UI.getInputFileFromLocalApp().readLine();
//            queue.sendMessage(myQueueUrl1, job);
//        }
//
//    }


        private String displayTextInputStream (InputStream input) throws IOException {
            String res = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                res = res + " " + line;
//            System.out.p  qsxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxsrintln("    " + line);
            }
//        System.out.println("");
            return res;
        }
    }
=======
}
>>>>>>> 3da014a8213aeda946170dfe27958ce5270cfeb5

