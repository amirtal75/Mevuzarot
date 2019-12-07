import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LocalApp {

    String loacalAppQueueUrl;
    String inputFile;
    BookList inputRepresentation;
    AWSCredentialsProvider credentialsProvider;

    public LocalApp(String loacalAppQueueUrl, String inputFile, AWSCredentialsProvider credentialsProvider) {
        this.loacalAppQueueUrl = loacalAppQueueUrl;
        this.inputFile = inputFile;
        this.credentialsProvider = credentialsProvider;
        this.inputRepresentation = new BookList();
    }

    public void run() throws Exception {

        String path = "/home/amirtal/IdeaProjects/Project1/src/main/java/";
        Gson gson = new Gson();
        ArrayList<parsedInputObject> inputList = parse(path + inputFile);
        String outputFilename = inputFile + UUID.randomUUID() + ".txt";

        BufferedWriter writer = new BufferedWriter(new FileWriter(path + outputFilename));
        for(parsedInputObject obj : inputList) {
            writer.write(obj.getReview().getId() + "@" + obj.getReview().getText() + "\n");
        }

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        S3Bucket s3 = new S3Bucket("assignment1", credentialsProvider);
        Queue queue = new Queue(credentialsProvider);

        // creating AWS resources, Asuuming local app queue was created by the wrapper
        ArrayList<Instance> Ids = getInstances(ec2);
        if (!hasManager(Ids, ec2)){

            createInstance(Ids, ec2,5,5, "");
            Ids = getInstances(ec2);
            createTags(ec2, Ids.get(0).getInstanceId(), new Tag("manager", "manager"));
        }


        terminateInstances(Ids, ec2);
        s3.createBucket();

        s3.upload(path, outputFilename);

        queue.sendMessage(loacalAppQueueUrl, outputFilename + "@" + s3.bucketName);
        Boolean managerAnswerNotReceived = true;
        Message answer = null;

        /*
        while (managerAnswerNotReceived){
            List<Message> messages = queue.recieveMessage(this.loacalAppQueueUrl);
            for (Message msg : messages) {
                if (msg.getBody().startsWith(inputFile)){
                    managerAnswerNotReceived = false;
                    queue.deleteMessage(this.loacalAppQueueUrl, msg);
                }
            }
            Thread.sleep(60);
        }

        if (answer == null){
            throw new Exception("Answer from Manager had an error in the file");
        }

        else{
            S3Object downObj = s3.downloadObject(outputFilename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(downObj.getObjectContent()));
            String ans[] = new String[2];
            ans = reader.readLine().split("@");
            inputRepresentation.addSentiment(ans[0],ans[1]);

            // !!!!!!! Need to complete !!!!!
            createHTML(inputRepresentation);
        }

         */

    }

    private void createHTML(BookList inputRepresentation){

    }

    private void toCHANGE(AmazonEC2 ec2, int min, int max, String bucketName, String localpath) {
        Boolean managerExist = false;

        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            List<TagDescription> list = ec2.describeTags().getTags();
            if (!list.isEmpty()) {
                for (TagDescription tag : list)
                    if (tag.getKey().equals("manager"))
                        managerExist = true;
            }
            if (!managerExist) {
                String data =  credentialsProvider.getCredentials().getAWSAccessKeyId() + "@" + credentialsProvider.getCredentials().getAWSSecretKey();
                RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", min, max);
                request.setInstanceType(InstanceType.T1Micro.toString());

                String userData = "";
                String downloadJar = "aws s3 cp s3://" + bucketName + "/manager.jar" + localpath + "manager.jar" + " --recursive;";
                String createCredentialsFile = "touch /home/amirtal/.aws/crentials;";
                String setAccessKey = "aws configure set aws_access_key_id default_access_key;";
                String setSecretAccessKey ="aws configure set aws_secret_access_key default_secret_key;";
                // request.withUserData(
                // + "java -jar manager.jar " + data);

                List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

                CreateTagsRequest createTagsRequest = new CreateTagsRequest();
                createTagsRequest.withResources("ami-0c5204531f799e0c6").withTags(new Tag("manager", "manager"));
                ec2.createTags(createTagsRequest);
            }

        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }
    private  boolean hasManager(ArrayList<Instance> Ids, AmazonEC2 ec2){
        for (Instance instance:
                Ids) {
            for (Tag tag:
                    instance.getTags()) {
                if (tag.getKey().equals("manager")){
                    return true;
                }
            }
        }
        return false;
    }

    private  void createTags(AmazonEC2 ec2, String instanceId, Tag tag){
        System.out.println(instanceId);
        CreateTagsRequest request = new CreateTagsRequest().withResources(instanceId).withTags(tag);
        CreateTagsResult response = ec2.createTags(request);
    }

    private  void createInstance(ArrayList<Instance> Ids, AmazonEC2 ec2, int min, int max, String userdata){
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", min, max);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.withUserData(userdata);
        RunInstancesResult instancesResult = ec2.runInstances(request);
    }

    private  void terminateInstances(ArrayList<Instance> Ids, AmazonEC2 ec2) {
        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            TerminateInstancesRequest terminateRequest;
            for (Instance instance:
                    Ids) {
                terminateRequest = new TerminateInstancesRequest();
                terminateRequest.withInstanceIds(instance.getInstanceId());
                ec2.terminateInstances(terminateRequest);
            }



        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    private  ArrayList<Instance> getInstances(AmazonEC2 ec2){
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        boolean notdone = true;

        ArrayList<Instance> Ids= new ArrayList<>();
        while(notdone) {
            DescribeInstancesResult response = ec2.describeInstances(request);
            List<Reservation> reservations = response.getReservations();
            if (reservations.isEmpty()){
                notdone = false;
            }
            else{
                for(Reservation reservation : reservations) {
                    //System.out.println(reservations.size());
                    List<Instance> instances = reservation.getInstances();
                    if (instances.isEmpty()){
                        notdone = false;
                    }
                    else{
                        //System.out.println(instances.size());
                        Ids.addAll(instances);
                    }
                }
            }


            request.setNextToken(response.getNextToken());

            if(response.getNextToken() == null) {
                notdone = false;
            }
        }
        return  Ids;
    }
    private ArrayList<parsedInputObject> parse(String filename) {

        ArrayList<parsedInputObject> inputArray = new ArrayList<parsedInputObject>();
        Gson gson = new Gson();
        BufferedReader reader;
        ArrayList<Review> reviews;
        try {
            reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                Book dataholder = gson.fromJson(line, Book.class);
                this.inputRepresentation.add(dataholder);

                if (dataholder != null) {
                    reviews = dataholder.getReviews();
                    for (int i = 0; i < reviews.size(); i++) {
                        inputArray.add(new parsedInputObject(dataholder.getTitle(), reviews.get(i)));
                    }
                }
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return inputArray;
    }
}