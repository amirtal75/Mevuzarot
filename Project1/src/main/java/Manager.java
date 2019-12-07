import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Manager {

    public void work( String QueueUrlLocalApps) throws Exception {

        AWSCredentialsProvider credentialsProvider= new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        Queue queue = new Queue(credentialsProvider);
        S3Bucket s3 = new S3Bucket("assignment1", credentialsProvider);
        ArrayList<upJobIndicator> upJobIndicatorList = new ArrayList<upJobIndicator>();
        String myQueueUrl1 = queue.createQueue();
        String myQueueUrl2 = queue.createQueue();

        List<Message> messages = queue.recieveMessage(QueueUrlLocalApps);

        for (Message message : messages) {
            String[] content = message.getBody().split("@");
            String line;
            for (int i = 0; i<1 ; i++){

                String key = content[0];
                String bucketName = content[1];
                System.out.println("Downloading an object with key: " + key + " from the bucket: " + bucketName);
                try {

                    S3Object object = s3.downloadObject(key);
                    upJobIndicatorList.add(new upJobIndicator( key,bucketName, object));

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // adding jobs to inputjobs queue
        String job;
        for (upJobIndicator UI:
                upJobIndicatorList) {
            job = UI.getInputFileFromLocalApp().readLine();
            queue.sendMessage(myQueueUrl1, job);
        }

    }


    private String  displayTextInputStream (InputStream input) throws IOException {
        String res ="";
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            res = res +" "+ line;
//            System.out.println("    " + line);
        }
//        System.out.println("");
        return res;
    }
}
