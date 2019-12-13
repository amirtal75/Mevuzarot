import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
//import javafx.concurrent.Task;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

    public static void main(String[] args) throws Exception {

        String summeryFilesIndicatorQueue = args[0];
        String QueueUrlLocalApps = args[1];
        boolean shouldTerminate = false;

        ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer, InputFileObject>();
        ArrayList<InputFileObject> InputFileObjectList = new ArrayList<InputFileObject>();//????
        ConcurrentHashMap<Integer, StringBuilder> stringResultsById = new ConcurrentHashMap<>(); // will be passed to the outputThread by constructor
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        Queue queue = new Queue();
        S3Bucket s3 = new S3Bucket("assignment1", credentialsProvider);
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
//        ArrayList<upJobIndicator> upJobIndicatorList = new ArrayList<upJobIndicator>();
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

            poolForInput.execute(new InputThread(QueueUrlLocalApps, myQueueUrl1, InputFileObjectById, credentialsProvider, inputFilename, bucketName));
            poolForOutput.execute(new OutputThread(myQueueUrl2, InputFileObjectById, credentialsProvider, stringResultsById));

            queue.deleteMessage(myQueueUrl1, currMessege);
        }

         poolForInput.shutdown();
         poolForOutput.shutdown();
    }


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
//            System.out.println("    " + line);
            }
//        System.out.println("");
            return res;
        }
    }

